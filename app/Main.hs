{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Protolude hiding
  ( MVar, putMVar, takeMVar, newMVar, newEmptyMVar, readMVar
  , atomically, STM(..), Chan, newTVar, readTVar, writeTVar
  , newChan, writeChan, readChan
  , threadDelay, killThread, TVar(..)
  , catch, handle, takeWhile, takeWhile1, (<|>)
  , lift
  )

import Control.Concurrent.Classy hiding (catch)
import Control.Monad.Fail
import Control.Monad.Catch
import Control.Monad.Trans.Class

import qualified Data.Map as Map
import qualified Data.List as L
import qualified Data.Set as Set
import qualified Data.Serialize as S

import Numeric.Natural

import System.Console.Repline
import Text.Read hiding (lift)
import System.Random
import qualified System.Directory as Directory

import Raft
import Raft.Config
import Raft.Log
import Raft.Log.PostgreSQL
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
  deriving (Show, Generic)

instance S.Serialize StoreCmd

type Store = Map Var Natural

instance RaftStateMachinePure Store StoreCmd where
  data RaftStateMachinePureError Store StoreCmd = StoreError Text deriving (Show)
  type RaftStateMachinePureCtx Store StoreCmd = ()

  rsmTransition _ store cmd =
    Right $ case cmd of
      Set x n -> Map.insert x n store
      Incr x -> Map.adjust succ x store

instance (Monad m, sm ~ Store, v ~ StoreCmd, RaftStateMachinePure sm v) => RaftStateMachine (RaftExampleM m sm v) sm v where
  validateCmd _ = pure (Right ())
  askRaftStateMachinePureCtx = pure ()

--------------------
-- Raft instances --
--------------------

data NodeEnv sm = NodeEnv
  { nEnvStore :: TVar (STM IO) sm
  , nEnvNodeId :: NodeId
  }

newtype RaftExampleM m sm v a = RaftExampleM {
    unRaftExampleM :: ReaderT (NodeEnv sm) (RaftSocketT v (RaftPersistFileStoreT m)) a
  }

deriving instance Functor m => Functor (RaftExampleM m sm v)
deriving instance Applicative m => Applicative (RaftExampleM m sm v)
deriving instance Monad m => Monad (RaftExampleM m sm v)
deriving instance MonadIO m => MonadIO (RaftExampleM m sm v)
deriving instance MonadFail m => MonadFail (RaftExampleM m sm v)
deriving instance Monad m => MonadReader (NodeEnv sm) (RaftExampleM m sm v)
deriving instance Alternative m => Alternative (RaftExampleM m sm v)
deriving instance MonadPlus m => MonadPlus (RaftExampleM m sm v)

deriving instance MonadThrow m => MonadThrow (RaftExampleM m sm v)
deriving instance MonadCatch m => MonadCatch (RaftExampleM m sm v)
deriving instance MonadMask m => MonadMask (RaftExampleM m sm v)
deriving instance MonadConc m => MonadConc (RaftExampleM m sm v)

runRaftExampleM
  :: (MonadIO m, MonadConc m)
  => NodeEnv sm
  -> NodeSocketEnv v
  -> RaftPersistFile
  -> RaftExampleM m sm v a
  -> m a
runRaftExampleM nodeEnv nodeSocketEnv raftPersistFile raftExampleM =
  flip runReaderT raftPersistFile . unRaftPersistFileStoreT $
    flip runReaderT nodeSocketEnv . unRaftSocketT $
        flip runReaderT nodeEnv $ unRaftExampleM raftExampleM

instance (MonadIO m, MonadConc m) => RaftSendClient (RaftExampleM m Store StoreCmd) Store StoreCmd where
  sendClient cid msg = (RaftExampleM . lift) $ sendClient cid msg

instance (MonadIO m, MonadConc m) => RaftRecvClient (RaftExampleM m Store StoreCmd) StoreCmd where
  type RaftRecvClientError (RaftExampleM m Store StoreCmd) StoreCmd = Text
  receiveClient = RaftExampleM $ lift receiveClient

instance (MonadIO m, MonadConc m) => RaftSendRPC (RaftExampleM m Store StoreCmd) StoreCmd where
  sendRPC nid msg = (RaftExampleM . lift) $ sendRPC nid msg

instance (MonadIO m, MonadConc m) => RaftRecvRPC (RaftExampleM m Store StoreCmd) StoreCmd where
  type RaftRecvRPCError (RaftExampleM m Store StoreCmd) StoreCmd = Text
  receiveRPC = RaftExampleM $ lift receiveRPC

instance (MonadIO m, MonadConc m) => RaftPersist (RaftExampleM m Store StoreCmd) where
  type RaftPersistError (RaftExampleM m Store StoreCmd) = RaftPersistFileStoreError
  initializePersistentState = RaftExampleM $ lift $ lift initializePersistentState
  writePersistentState ps = RaftExampleM $ lift $ lift $ writePersistentState ps
  readPersistentState = RaftExampleM $ lift $ lift $ readPersistentState

instance (MonadConc m, RaftInitLog m StoreCmd) => RaftInitLog (RaftExampleM m Store StoreCmd) StoreCmd where
  type RaftInitLogError (RaftExampleM m Store StoreCmd) = RaftInitLogError m
  initializeLog p = RaftExampleM $ lift $ lift $ lift $ initializeLog p

instance RaftWriteLog m StoreCmd => RaftWriteLog (RaftExampleM m Store StoreCmd) StoreCmd where
  type RaftWriteLogError (RaftExampleM m Store StoreCmd) = RaftWriteLogError m
  writeLogEntries entries = RaftExampleM $ lift $ lift $ lift $ writeLogEntries entries

instance RaftReadLog m StoreCmd => RaftReadLog (RaftExampleM m Store StoreCmd) StoreCmd where
  type RaftReadLogError (RaftExampleM m Store StoreCmd) = RaftReadLogError m
  readLogEntry idx = RaftExampleM $ lift $ lift $ lift $ readLogEntry idx
  readLastLogEntry = RaftExampleM $ lift $ lift $ lift $ readLastLogEntry

instance RaftDeleteLog m StoreCmd => RaftDeleteLog (RaftExampleM m Store StoreCmd) StoreCmd where
  type RaftDeleteLogError (RaftExampleM m Store StoreCmd) = RaftDeleteLogError m
  deleteLogEntriesFrom idx = RaftExampleM $ lift $ lift $ lift $ deleteLogEntriesFrom idx

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
  } deriving (Functor, Applicative, Monad, MonadIO)

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
    args <- (toS <$>) <$> getArgs
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
        nEnv <- initNodeEnv nid
        nPersistFile <- RaftPersistFile <$> persistentFilePath nid
        case storageType of
          FileStore -> do
            nLogsFile <- RaftLogFile <$> logsFilePath nid
            runRaftLogFileStoreT nLogsFile $
              runRaftNode' nSocketEnv nEnv nPersistFile
          PostgreSQL dbName -> do
            let pgConnInfo = raftDatabaseConnInfo "libraft_test" "libraft_test" dbName
            runRaftPostgresM pgConnInfo $
              runRaftNode' nSocketEnv nEnv nPersistFile
     where
        runRaftNode'
          :: ( MonadIO m, MonadConc m, MonadFail m
             , RaftInitLog m StoreCmd, RaftReadLog m StoreCmd, RaftWriteLog m StoreCmd
             , RaftDeleteLog m StoreCmd, Exception (RaftInitLogError m), Exception (RaftReadLogError m)
             , Exception (RaftWriteLogError m), Exception (RaftDeleteLogError m), Typeable m
             )
          => NodeSocketEnv StoreCmd
          -> NodeEnv Store
          -> RaftPersistFile
          -> m ()
        runRaftNode' nSocketEnv nEnv nPersistFile =
          runRaftExampleM nEnv nSocketEnv nPersistFile $ do
            let allNodeIds = Set.fromList (nid : nids)
            let (host, port) = RS.nidToHostPort (toS nid)
            let nodeConfig = NodeConfig
                              { configNodeId = toS nid
                              , configNodeIds = allNodeIds
                              -- These are recommended timeouts from the original
                              -- raft paper and the ARC report.
                              , configElectionTimeout = (150000, 300000)
                              , configHeartbeatTimeout = 50000
                              , configStorageState = storageState
                              }
            fork $ RaftExampleM $ lift (acceptConnections host port)
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

    initNodeEnv :: NodeId -> IO (NodeEnv Store)
    initNodeEnv nid = do
      let (host, port) = RS.nidToHostPort (toS nid)
      storeTVar <- atomically (newTVar mempty)
      pure NodeEnv
        { nEnvStore = storeTVar
        , nEnvNodeId = toS host <> ":" <> toS port
        }

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
