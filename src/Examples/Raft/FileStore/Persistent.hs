{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Examples.Raft.FileStore.Persistent where

import Protolude hiding (try)

import Control.Monad.Fail
import Control.Monad.Catch
import Control.Monad.Trans.Class

import qualified Data.ByteString as BS
import qualified Data.Serialize as S

import System.Directory (doesFileExist)
import qualified System.AtomicWrite.Writer.ByteString as AW

import Raft.Client
import Raft.Log
import Raft.Monad
import Raft.Persistent
import Raft.RPC
import Raft.StateMachine

newtype RaftPersistFileStoreError = RaftPersistFileStoreError Text
  deriving (Show)

newtype RaftPersistFile = RaftPersistFile FilePath

instance Exception RaftPersistFileStoreError

newtype RaftPersistFileStoreT m a = RaftPersistFileStoreT { unRaftPersistFileStoreT :: ReaderT RaftPersistFile m a }
  deriving (Functor, Applicative, Monad, MonadIO, MonadFail, MonadReader RaftPersistFile, Alternative, MonadPlus, MonadTrans)

deriving instance MonadThrow m => MonadThrow (RaftPersistFileStoreT m)
deriving instance MonadCatch m => MonadCatch (RaftPersistFileStoreT m)
deriving instance MonadMask m => MonadMask (RaftPersistFileStoreT m)

runRaftPersistFileStoreT :: RaftPersistFile -> RaftPersistFileStoreT m a -> m a
runRaftPersistFileStoreT persistFile = flip runReaderT persistFile . unRaftPersistFileStoreT

--------------------------------------------------------------------------------
-- Raft Instances
--------------------------------------------------------------------------------

-- A RaftPersist instance for the RaftPersistFileStoreT monad.
--
-- Warning: `atomicWriteFile` causes large pauses on the order of hundreds of
-- milliseconds. This can cause leadership to change since the node become
-- unresponsive for much longer than the recommended election timeouts.
instance MonadIO m => RaftPersist (RaftPersistFileStoreT m) where
  type RaftPersistError (RaftPersistFileStoreT m) = RaftPersistFileStoreError

  initializePersistentState = do
    RaftPersistFile psFile <- ask
    fileExists <- liftIO $ doesFileExist psFile
    if fileExists
      then pure $ Left (RaftPersistFileStoreError ("Persistent file " <> toS psFile <> " already exists!"))
      else do
        eRes <- liftIO . try $
          AW.atomicWriteFile psFile (toS (S.encode initPersistentState))
        case eRes of
          Left (err :: SomeException) -> do
            let errPrefix = "Failed initializing persistent storage: "
            pure (Left (RaftPersistFileStoreError (errPrefix <> show err)))
          Right _ -> pure (Right ())

  writePersistentState ps = do
    RaftPersistFile psFile <- ask
    liftIO $ Right <$> AW.atomicWriteFile psFile (S.encode ps)

  readPersistentState = do
    RaftPersistFile psFile <- ask
    fileContent <- liftIO $ BS.readFile psFile
    case S.decode fileContent of
      Left err -> panic (toS $ "readPersistentState: " ++ err)
      Right ps -> pure $ Right ps

--------------------------------------------------------------------------------
-- Inherited Instances
--------------------------------------------------------------------------------

instance RaftStateMachine m sm v => RaftStateMachine (RaftPersistFileStoreT m) sm v where
  validateCmd = lift . validateCmd
  askRaftStateMachinePureCtx = lift askRaftStateMachinePureCtx

instance (MonadIO m, MonadMask m, RaftSendClient m sm v) => RaftSendClient (RaftPersistFileStoreT m) sm v where
  sendClient cid msg = lift $ sendClient cid msg

instance (MonadIO m, RaftRecvClient m v) => RaftRecvClient (RaftPersistFileStoreT m) v where
  type RaftRecvClientError (RaftPersistFileStoreT m) v = RaftRecvClientError m v
  receiveClient = lift receiveClient

instance (MonadIO m, MonadMask m, RaftSendRPC m v) => RaftSendRPC (RaftPersistFileStoreT m) v where
  sendRPC nid msg = lift $ sendRPC nid msg

instance (MonadIO m, RaftRecvRPC m v) => RaftRecvRPC (RaftPersistFileStoreT m) v where
  type RaftRecvRPCError (RaftPersistFileStoreT m) v = RaftRecvRPCError m v
  receiveRPC = lift receiveRPC

instance (MonadIO m, RaftInitLog m v) => RaftInitLog (RaftPersistFileStoreT m) v where
  type RaftInitLogError (RaftPersistFileStoreT m) = RaftInitLogError m
  initializeLog p = lift $ initializeLog p

instance RaftWriteLog m v => RaftWriteLog (RaftPersistFileStoreT m) v where
  type RaftWriteLogError (RaftPersistFileStoreT m) = RaftWriteLogError m
  writeLogEntries entries = lift $ writeLogEntries entries

instance RaftReadLog m v => RaftReadLog (RaftPersistFileStoreT m) v where
  type RaftReadLogError (RaftPersistFileStoreT m) = RaftReadLogError m
  readLogEntry idx = lift $ readLogEntry idx
  readLastLogEntry = lift readLastLogEntry

instance RaftDeleteLog m v => RaftDeleteLog (RaftPersistFileStoreT m) v where
  type RaftDeleteLogError (RaftPersistFileStoreT m) = RaftDeleteLogError m
  deleteLogEntriesFrom idx = lift $ deleteLogEntriesFrom idx

instance MonadRaftChan v m => MonadRaftChan v (RaftPersistFileStoreT m) where
  type RaftEventChan v (RaftPersistFileStoreT m) = RaftEventChan v m
  readRaftChan = lift . readRaftChan
  writeRaftChan chan = lift . writeRaftChan chan
  newRaftChan = lift (newRaftChan @v @m)

instance (MonadIO m, MonadRaftFork m) => MonadRaftFork (RaftPersistFileStoreT m) where
  type RaftThreadId (RaftPersistFileStoreT m) = RaftThreadId m
  raftFork r m = do
    persistFile <- ask
    lift $ raftFork r (runRaftPersistFileStoreT persistFile m)
