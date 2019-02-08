{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Protolude

import Control.Concurrent.Lifted (fork)
import Control.Concurrent.STM.TChan

import Control.Monad.Fail
import Control.Monad.Catch
import Control.Monad.Trans.Class

import qualified Data.Map as Map
import qualified Data.List as L
import qualified Data.Set as Set
import Data.Serialize (Serialize)

import Numeric.Natural

import System.Console.Repline
import Text.Read hiding (lift)
import System.Random
import qualified System.Directory as Directory

import Raft
import Raft.Config
import Raft.Log
import Raft.Log.PostgreSQL
import Raft.Monad
import Raft.Client

import Database.PostgreSQL.Simple

import qualified Examples.Raft.Socket.Client as RS
import Examples.Raft.Socket.Node
import qualified Examples.Raft.Socket.Common as RS
import Examples.Raft.FileStore.Log
import Examples.Raft.FileStore.Persistent

------------------------------
-- State Machine & Commands --
------------------------------

-- State machine with two basic operations: set a variable to a value and
-- increment value

type Var = ByteString

data StoreCmd
  = Set Var Natural
  | Incr Var
  deriving stock (Show, Generic)
  deriving anyclass Serialize

type Store = Map Var Natural

instance RaftStateMachinePure Store StoreCmd where
  data RaftStateMachinePureError Store StoreCmd = StoreError Text deriving (Show)
  type RaftStateMachinePureCtx Store StoreCmd = ()

  rsmTransition _ store cmd =
    Right $ case cmd of
      Set x n -> Map.insert x n store
      Incr x -> Map.adjust succ x store

--------------------------------------------------------------------------------
-- Raft Example Monad Transformer
--------------------------------------------------------------------------------

newtype RaftExampleT m a = RaftExampleT { unRaftExampleT :: m a }
  deriving newtype (Functor, Applicative, Monad, MonadIO, MonadFail, MonadMask, MonadCatch, MonadThrow)

instance MonadTrans RaftExampleT where
  lift = RaftExampleT

instance (Monad m, RaftStateMachinePure Store StoreCmd) => RaftStateMachine (RaftExampleT m) Store StoreCmd where
  validateCmd _ = pure (Right ())
  askRaftStateMachinePureCtx = pure ()

instance MonadRaftChan v m => MonadRaftChan v (RaftExampleT m) where
  type RaftEventChan v (RaftExampleT m) = RaftEventChan v m
  readRaftChan = lift . readRaftChan
  writeRaftChan chan = lift . writeRaftChan chan
  newRaftChan = lift (newRaftChan @v @m)

instance (MonadIO m, MonadRaftFork m) => MonadRaftFork (RaftExampleT m) where
  type RaftThreadId (RaftExampleT m) = RaftThreadId m
  raftFork r m = lift $ raftFork r (runRaftExampleT m)

runRaftExampleT :: RaftExampleT m a -> m a
runRaftExampleT = unRaftExampleT

--------------------
-- Client console --
--------------------

-- Clients interact with the nodes from a terminal:
-- Accepted operations are:
-- - addNode <host:port>
--      Add nodeId to the set of nodeIds that the client will communicate with
-- - getNodes
--      Return the node ids that the client is aware of
-- - read
--      Return the state of the leader
-- - set <var> <val>
--      Set variable to a specific value
-- - incr <var>
--      Increment the value of a variable

newtype ConsoleM a = ConsoleM
  { unConsoleM :: HaskelineT (RS.RaftSocketClientM Store StoreCmd) a
  } deriving newtype (Functor, Applicative, Monad, MonadIO)

liftRSCM = ConsoleM . lift

-- | Evaluate and handle each line user inputs
handleConsoleCmd :: [Char] -> ConsoleM ()
handleConsoleCmd input = do
  nids <- liftRSCM clientGetNodes
  case L.words input of
    ["addNode", nid] -> liftRSCM $ clientAddNode (toS nid)
    ["getNodes"] -> print =<< liftRSCM clientGetNodes
    ["read"] ->
      ifNodesAdded nids $
        handleResponse =<< liftRSCM (RS.socketClientRead ClientReadStateMachine)
    ["read", n] ->
      ifNodesAdded nids $
        handleResponse =<< liftRSCM (RS.socketClientRead (ClientReadEntries (ByIndex (Index (read n)))))
    ["read", "[", low, high, "]" ] ->
      ifNodesAdded nids $ do
        let byInterval = ByIndices $ IndexInterval (Just (Index (read low))) (Just (Index (read high)))
        handleResponse =<< liftRSCM (RS.socketClientRead (ClientReadEntries byInterval))
    ["incr", cmd] ->
      ifNodesAdded nids $
        handleResponse =<< liftRSCM (RS.socketClientWrite (Incr (toS cmd)))
    ["set", var, val] ->
      ifNodesAdded nids $
        handleResponse =<< liftRSCM (RS.socketClientWrite (Set (toS var) (read val)))
    _ -> print "Invalid command. Press <TAB> to see valid commands"

  where
    ifNodesAdded nids m
      | nids == Set.empty =
          putText "Please add some nodes to query first. Eg. `addNode localhost:3001`"
      | otherwise = m

    handleResponse :: Show a => Either Text a -> ConsoleM ()
    handleResponse res = do
      case res of
        Left err -> liftIO $ putText err
        Right resp -> liftIO $ putText (show resp)

data LogStorage = FileStore | PostgreSQL [Char]

main :: IO ()
main = do
    args :: [ByteString] <- fmap toS <$> getArgs
    case args of
      ["client"] -> clientMain
      ("node":"fresh":"file":nid:nids) -> initNode New FileStore (nid:nids)
      ("node":"existing":"file":nid:nids) -> initNode Existing FileStore (nid:nids)
      ("node":"fresh":"postgres":nm:nid:nids) -> initNode New (PostgreSQL $ toS nm) (nid:nids)
      ("node":"existing":"postgres":nm:nid:nids) -> initNode Existing (PostgreSQL $ toS nm) (nid:nids)
  where
    initNode storageState storageType (nid:nids) = do
        nodeDir <- mkExampleDir nid
        case storageState of
          New -> cleanStorage nodeDir storageType
          Existing -> pure ()
        nSocketEnv <- initSocketEnv nid
        nPersistFile <- RaftPersistFile <$> persistentFilePath nid

        let (host, port) = RS.nidToHostPort (toS nid)
        -- Launch the server receiving connections from raft other nodes
        fork $ flip runReaderT nSocketEnv . unRaftSocketT $
          acceptConnections host port

        case storageType of
          FileStore -> do
            nLogsFile <- RaftLogFile <$> logsFilePath nid
            runRaftExampleT $
              runRaftLogFileStoreT nLogsFile $
                runRaftNode' nSocketEnv nPersistFile
          PostgreSQL dbName -> do
            let pgConnInfo = raftDatabaseConnInfo "libraft_test" "libraft_test" dbName
            runRaftExampleT $
              runRaftPostgresT pgConnInfo $
                runRaftNode' nSocketEnv nPersistFile
     where
        runRaftNode'
          :: ( MonadIO m, MonadRaft StoreCmd m, MonadFail m, MonadMask m
             , MonadRaft StoreCmd m, RaftStateMachine m Store StoreCmd
             , RaftInitLog m StoreCmd, RaftReadLog m StoreCmd, RaftWriteLog m StoreCmd, RaftDeleteLog m StoreCmd
             , Exception (RaftInitLogError m), Exception (RaftReadLogError m)
             , Exception (RaftWriteLogError m), Exception (RaftDeleteLogError m)
             , Typeable m
             )
          => NodeSocketEnv StoreCmd
          -> RaftPersistFile
          -> m ()
        runRaftNode' nSocketEnv nPersistFile =
          runRaftSocketT nSocketEnv $
            runRaftPersistFileStoreT nPersistFile $ do
              let allNodeIds = Set.fromList (nid : nids)
              let nodeConfig = RaftNodeConfig
                                { configNodeId = toS nid
                                , configNodeIds = allNodeIds
                                -- These are recommended timeouts from the original
                                -- raft paper and the ARC report.
                                , configElectionTimeout = (150000, 300000)
                                , configHeartbeatTimeout = 50000
                                , configStorageState = storageState
                                }
              electionTimerSeed <- liftIO randomIO
              runRaftNode nodeConfig (LogCtx LogStdout Debug) electionTimerSeed (mempty :: Store)

    cleanStorage :: FilePath -> LogStorage -> IO ()
    cleanStorage nodeDir ls = do
      case ls of
        PostgreSQL dbName -> do
          Control.Monad.Catch.bracket (connect initConnInfo) close $ \conn ->
            void $ deleteDB (raftDatabaseName dbName) conn
        _ -> pure ()
      Directory.removePathForcibly nodeDir
      Directory.createDirectoryIfMissing False nodeDir

    persistentFilePath :: NodeId -> IO FilePath
    persistentFilePath nid = do
      tmpDir <- Directory.getTemporaryDirectory
      pure $ tmpDir ++ "/" ++ toS nid ++ "/" ++ "persistent"

    logsFilePath :: NodeId -> IO FilePath
    logsFilePath nid = do
      tmpDir <- Directory.getTemporaryDirectory
      pure (tmpDir ++ "/" ++ toS nid ++ "/" ++ "logs")

    initSocketEnv :: NodeId -> IO (NodeSocketEnv v)
    initSocketEnv nid = do
      msgQueue <- atomically newTChan
      clientReqQueue <- atomically newTChan
      pure NodeSocketEnv
        { nsMsgQueue = msgQueue
        , nsClientReqQueue = clientReqQueue
        }

    mkExampleDir :: NodeId -> IO FilePath
    mkExampleDir nid = do
      tmpDir <- Directory.getTemporaryDirectory
      let nodeDir = tmpDir ++ "/" ++ toS nid
      pure nodeDir

    clientMain :: IO ()
    clientMain = do
      let clientHost = "localhost"
      clientPort <- RS.getFreePort
      let clientId = ClientId $ RS.hostPortToNid (clientHost, clientPort)
      clientRespChan <- RS.newClientRespChan
      RS.runRaftSocketClientM clientId mempty clientRespChan $ do
        fork (lift (RS.clientResponseServer clientHost clientPort))
        evalRepl (pure ">>> ") (unConsoleM . handleConsoleCmd) [] Nothing (Word completer) (pure ())

    -- Tab Completion: return a completion for partial words entered
    completer :: Monad m => WordCompleter m
    completer n = do
      let cmds = ["addNode <host:port>", "getNodes", "incr <var>", "set <var> <val>"]
      return $ filter (isPrefixOf n) cmds
