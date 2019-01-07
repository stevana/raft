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

import qualified Examples.Raft.Socket.Client as RS
import qualified Examples.Raft.Socket.Node as RS
import Examples.Raft.Socket.Node
import qualified Examples.Raft.Socket.Common as RS

import Examples.Raft.FileStore
import Raft
import Raft.Client

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

instance RSMP Store StoreCmd where
  data RSMPError Store StoreCmd = StoreError Text deriving (Show)
  type RSMPCtx Store StoreCmd = ()

  applyCmdRSMP _ store cmd =
    Right $ case cmd of
      Set x n -> Map.insert x n store
      Incr x -> Map.adjust succ x store

instance (sm ~ Store, v ~ StoreCmd, RSMP sm v) => RSM sm v (RaftExampleM sm v) where
  validateCmd _ = pure (Right ())
  askRSMPCtx = pure ()

--------------------
-- Raft instances --
--------------------

data NodeEnv sm = NodeEnv
  { nEnvStore :: TVar (STM IO) sm
  , nEnvNodeId :: NodeId
  }

newtype RaftExampleM sm v a = RaftExampleM { unRaftExampleM :: ReaderT (NodeEnv sm) (RaftSocketT v (RaftFileStoreT IO)) a }
  deriving (Functor, Applicative, Monad, MonadIO, MonadFail, MonadReader (NodeEnv sm), Alternative, MonadPlus)

deriving instance MonadThrow (RaftExampleM sm v)
deriving instance MonadCatch (RaftExampleM sm v)
deriving instance MonadMask (RaftExampleM sm v)
deriving instance MonadConc (RaftExampleM sm v)

runRaftExampleM :: NodeEnv sm -> NodeSocketEnv v -> NodeFileStoreEnv -> RaftExampleM sm v a -> IO a
runRaftExampleM nodeEnv nodeSocketEnv nodeFileStoreEnv raftExampleM =
  runReaderT (unRaftFileStoreT $
    runReaderT (unRaftSocketT $
      runReaderT (unRaftExampleM raftExampleM) nodeEnv) nodeSocketEnv)
        nodeFileStoreEnv

instance RaftSendClient (RaftExampleM Store StoreCmd) Store where
  sendClient cid msg = (RaftExampleM . lift) $ sendClient cid msg

instance RaftRecvClient (RaftExampleM Store StoreCmd) StoreCmd where
  type RaftRecvClientError (RaftExampleM Store StoreCmd) StoreCmd = Text
  receiveClient = RaftExampleM $ lift receiveClient

instance RaftSendRPC (RaftExampleM Store StoreCmd) StoreCmd where
  sendRPC nid msg = (RaftExampleM . lift) $ sendRPC nid msg

instance RaftRecvRPC (RaftExampleM Store StoreCmd) StoreCmd where
  type RaftRecvRPCError (RaftExampleM Store StoreCmd) StoreCmd = Text
  receiveRPC = RaftExampleM $ lift receiveRPC

instance RaftWriteLog (RaftExampleM Store StoreCmd) StoreCmd where
  type RaftWriteLogError (RaftExampleM Store StoreCmd) = NodeEnvError
  writeLogEntries entries = RaftExampleM $ lift $ RaftSocketT (lift $ writeLogEntries entries)

instance RaftPersist (RaftExampleM Store StoreCmd) where
  type RaftPersistError (RaftExampleM Store StoreCmd) = NodeEnvError
  writePersistentState ps = RaftExampleM $ lift $ RaftSocketT (lift $ writePersistentState ps)
  readPersistentState = RaftExampleM $ lift $ RaftSocketT (lift $ readPersistentState)

instance RaftReadLog (RaftExampleM Store StoreCmd) StoreCmd where
  type RaftReadLogError (RaftExampleM Store StoreCmd) = NodeEnvError
  readLogEntry idx = RaftExampleM $ lift $ RaftSocketT (lift $ readLogEntry idx)
  readLastLogEntry = RaftExampleM $ lift $ RaftSocketT (lift readLastLogEntry)

instance RaftDeleteLog (RaftExampleM Store StoreCmd) StoreCmd where
  type RaftDeleteLogError (RaftExampleM Store StoreCmd) = NodeEnvError
  deleteLogEntriesFrom idx = RaftExampleM $ lift $ RaftSocketT (lift $ deleteLogEntriesFrom idx)

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
        handleResponse =<< liftRSCM RS.socketClientRead
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

main :: IO ()
main = do
    args <- (toS <$>) <$> getArgs
    case args of
      ["client"] -> clientMain
      ("node":"fresh":nid:nids) -> do
        removeExampleFiles nid
        createExampleFiles nid
        initNode (nid:nids)
      ("node":"existing":nid:nids) -> do
        createExampleFiles nid
        initNode (nid:nids)
  where
    initNode (nid:nids) = do
      nSocketEnv <- initSocketEnv nid
      nPersistentEnv <- initRaftFileStoreEnv nid
      nEnv <- initNodeEnv nid
      runRaftExampleM nEnv nSocketEnv nPersistentEnv $ do
        let allNodeIds = Set.fromList (nid : nids)
        let (host, port) = RS.nidToHostPort (toS nid)
        let nodeConfig = NodeConfig
                          { configNodeId = toS nid
                          , configNodeIds = allNodeIds
                          -- The election timeout must currently be an order of
                          -- magnitude greater than the recommended timeout
                          -- range of 150ms - 300ms due to an unresolved issue.
                          , configElectionTimeout = (1500000, 3000000)
                          , configHeartbeatTimeout = 200000
                          }
        fork $ RaftExampleM $ lift (acceptConnections host port)
        electionTimerSeed <- liftIO randomIO
        runRaftNode nodeConfig LogStdout electionTimerSeed (mempty :: Store)

    initPersistentFile :: NodeId -> IO ()
    initPersistentFile nid = do
      psPath <- persistentFile nid
      fileExists <- Directory.doesFileExist psPath
      when (not fileExists) $
        writeFile psPath (toS $ S.encode initPersistentState)

    persistentFile :: NodeId -> IO FilePath
    persistentFile nid = do
      tmpDir <- Directory.getTemporaryDirectory
      pure $ tmpDir ++ "/" ++ toS nid ++ "/" ++ "persistent"

    initLogsFile :: NodeId -> IO ()
    initLogsFile nid = do
      logsPath <- logsFile nid
      fileExists <- Directory.doesFileExist logsPath
      when (not fileExists) $
        writeFile logsPath (toS $ S.encode (mempty :: Entries StoreCmd))

    logsFile :: NodeId -> IO FilePath
    logsFile nid = do
      tmpDir <- Directory.getTemporaryDirectory
      pure (tmpDir ++ "/" ++ toS nid ++ "/" ++ "logs")

    createExampleFiles :: NodeId -> IO ()
    createExampleFiles nid = void $ do
      tmpDir <- Directory.getTemporaryDirectory
      Directory.createDirectoryIfMissing False (tmpDir ++ "/" ++ toS nid)
      initPersistentFile nid
      initLogsFile nid

    removeExampleFiles :: NodeId -> IO ()
    removeExampleFiles nid = handle (const (pure ()) :: SomeException -> IO ()) $ do
      tmpDir <- Directory.getTemporaryDirectory
      Directory.removeDirectoryRecursive (tmpDir ++ "/" ++ toS nid)

    initNodeEnv :: NodeId -> IO (NodeEnv Store)
    initNodeEnv nid = do
      let (host, port) = RS.nidToHostPort (toS nid)
      storeTVar <- atomically (newTVar mempty)
      pure NodeEnv
        { nEnvStore = storeTVar
        , nEnvNodeId = toS host <> ":" <> toS port
        }

    initRaftFileStoreEnv :: NodeId -> IO NodeFileStoreEnv
    initRaftFileStoreEnv nid = do
      psPath <- persistentFile nid
      psLogs <- logsFile nid
      pure NodeFileStoreEnv
            { nfsPersistentState = psPath
            , nfsLogEntries = psLogs
            }

    initSocketEnv :: NodeId -> IO (NodeSocketEnv v)
    initSocketEnv nid = do
      msgQueue <- atomically newTChan
      clientReqQueue <- atomically newTChan
      pure NodeSocketEnv
        { nsMsgQueue = msgQueue
        , nsClientReqQueue = clientReqQueue
        }

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
