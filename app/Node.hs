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

module Node where
import Protolude

import Control.Concurrent.Lifted (fork)
import Control.Concurrent.STM.TChan
import Control.Concurrent.STM.TVar

import Control.Monad.Fail
import Control.Monad.Catch

import qualified Data.Set as Set
import qualified Data.Map as Map

import System.FilePath ((</>))
import qualified System.Directory as Directory

import Database.PostgreSQL.Simple

import Raft
import Raft.Monad
import Raft.Config
import Raft.Log
import Raft.Log.PostgreSQL

import Examples.Raft.Socket.Node
import qualified Examples.Raft.Socket.Common as RS
import Examples.Raft.FileStore.Log
import Examples.Raft.FileStore.Persistent

import Store
import RaftExampleT

--------------------
-- Node --
--------------------

data LogStorage = FileStore | PostgreSQL ConnectInfo

nodeMain :: StorageState -> LogStorage -> NodeId -> [NodeId] -> IO ()
nodeMain storageState storageType nid nids = do
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
        PostgreSQL connInfo -> do
          runRaftExampleT $
            runRaftPostgresT connInfo $
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
        => NodeSocketEnv Store StoreCmd
        -> RaftPersistFile
        -> m ()
      runRaftNode' nSocketEnv nPersistFile =
        runRaftSocketT nSocketEnv $
          runRaftPersistFileStoreT nPersistFile $ do
            let allNodeIds = Set.fromList (nid : nids)
            let nodeConfig = RaftNodeConfig
                              { raftConfigNodeId = toS nid
                              , raftConfigNodeIds = allNodeIds
                              -- These are recommended timeouts from the original
                              -- raft paper and the ARC report.
                              , raftConfigElectionTimeout = (150000, 300000)
                              , raftConfigHeartbeatTimeout = 50000
                              , raftConfigStorageState = storageState
                              }
            runRaftNode nodeConfig defaultOptionalRaftNodeConfig (LogCtx LogStdout Debug) (mempty :: Store)

cleanStorage :: FilePath -> LogStorage -> IO ()
cleanStorage nodeDir ls = do
  case ls of
    PostgreSQL connInfo -> do
      Control.Monad.Catch.bracket (connect initConnInfo) close $ \conn ->
        void $ deleteDB (connectDatabase connInfo) conn
    _ -> pure ()
  Directory.removePathForcibly nodeDir
  Directory.createDirectoryIfMissing False nodeDir

persistentFilePath :: NodeId -> IO FilePath
persistentFilePath nid = do
  tmpDir <- Directory.getTemporaryDirectory
  pure $ tmpDir </> toS nid </> "persistent"

logsFilePath :: NodeId -> IO FilePath
logsFilePath nid = do
  tmpDir <- Directory.getTemporaryDirectory
  pure (tmpDir </> toS nid </> "logs")

initSocketEnv :: NodeId -> IO (NodeSocketEnv sm v)
initSocketEnv nid = do
  msgQueue <- atomically newTChan
  clientReqQueue <- atomically newTChan
  clientReqResps <- atomically $ newTVar Map.empty
  pure NodeSocketEnv
    { nsMsgQueue = msgQueue
    , nsClientReqQueue = clientReqQueue
    , nsClientReqResps = clientReqResps
    }

mkExampleDir :: NodeId -> IO FilePath
mkExampleDir nid = do
  tmpDir <- Directory.getTemporaryDirectory
  let nodeDir = tmpDir </> toS nid
  pure nodeDir
