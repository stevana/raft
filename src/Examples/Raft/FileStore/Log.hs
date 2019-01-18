{-# LANGUAGE TypeSynonymInstances #-}
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

module Examples.Raft.FileStore.Log where

import Protolude hiding (try)

import Control.Concurrent.Classy hiding (catch, ThreadId)
import Control.Monad.Fail
import Control.Monad.Catch
import Control.Monad.Trans.Class

import qualified Data.ByteString as BS
import Data.Sequence ((><))
import qualified Data.Sequence as Seq
import qualified Data.Serialize as S

import qualified System.AtomicWrite.Writer.ByteString as AW

import Raft.Log
import Raft.Types

newtype RaftLogFileStoreError = RaftLogFileStoreError Text
  deriving (Show)

newtype RaftLogFile = RaftLogFile { unRaftLogFile :: FilePath }

instance Exception RaftLogFileStoreError

newtype RaftLogFileStoreT m a = RaftLogFileStoreT { unRaftLogFileStoreT :: ReaderT RaftLogFile m a }
  deriving (Functor, Applicative, Monad, MonadIO, MonadFail, MonadReader RaftLogFile, Alternative, MonadPlus, MonadTrans)

deriving instance MonadThrow m => MonadThrow (RaftLogFileStoreT m)
deriving instance MonadCatch m => MonadCatch (RaftLogFileStoreT m)
deriving instance MonadMask m => MonadMask (RaftLogFileStoreT m)
deriving instance MonadConc m => MonadConc (RaftLogFileStoreT m)

runRaftLogFileStoreT :: RaftLogFile -> RaftLogFileStoreT m a -> m a
runRaftLogFileStoreT rlogFile = flip runReaderT rlogFile . unRaftLogFileStoreT

instance (S.Serialize v, MonadIO m, MonadConc m) => RaftInitLog (RaftLogFileStoreT m) v where
  type RaftInitLogError (RaftLogFileStoreT m) = RaftLogFileStoreError
  initializeLog _ = do
    RaftLogFile logFile <- ask
    eRes <- liftIO $ try (BS.writeFile logFile (S.encode (mempty :: Entries v)))
    case eRes of
      Left (e :: SomeException) -> pure $ Left (RaftLogFileStoreError (show e))
      Right _ -> pure $ Right ()

instance (MonadIO m, MonadConc m, S.Serialize v) => RaftWriteLog (RaftLogFileStoreT m) v where
  type RaftWriteLogError (RaftLogFileStoreT m) = RaftLogFileStoreError
  writeLogEntries newEntries = do
    RaftLogFile logFile <- ask
    eLogEntries <- readLogEntries
    case eLogEntries of
      Left err -> panic ("writeLogEntries: " <> err)
      Right currEntries -> liftIO $ Right <$> AW.atomicWriteFile logFile (S.encode (currEntries >< newEntries))

instance (MonadIO m, MonadConc m, S.Serialize v) => RaftReadLog (RaftLogFileStoreT m) v where
  type RaftReadLogError (RaftLogFileStoreT m) = RaftLogFileStoreError
  readLogEntry (Index idx) = do
    eLogEntries <- readLogEntries
    case eLogEntries of
      Left err -> panic ("readLogEntry: " <> err)
      Right entries ->
        case entries Seq.!? fromIntegral (if idx == 0 then 0 else idx - 1) of
          Nothing -> pure (Right Nothing)
          Just e -> pure (Right (Just e))

  readLastLogEntry = do
    eLogEntries <- readLogEntries
    case eLogEntries of
      Left err -> panic ("readLastLogEntry: " <> err)
      Right entries -> case entries of
        Seq.Empty -> pure (Right Nothing)
        (_ Seq.:|> e) -> pure (Right (Just e))

instance (MonadIO m, MonadConc m, S.Serialize v) => RaftDeleteLog (RaftLogFileStoreT m) v where
  type RaftDeleteLogError (RaftLogFileStoreT m) = RaftLogFileStoreError
  deleteLogEntriesFrom idx = do
    eLogEntries <- readLogEntries
    case eLogEntries of
      Left err -> panic ("deleteLogEntriesFrom: " <> err)
      Right (entries :: Entries v) -> do
        let newLogEntries = Seq.dropWhileR ((>= idx) . entryIndex) entries
        RaftLogFile logFile <- ask
        liftIO $ AW.atomicWriteFile logFile (S.encode newLogEntries)
        pure (Right DeleteSuccess)

readLogEntries :: (MonadIO m, S.Serialize v) => RaftLogFileStoreT m (Either Text (Entries v))
readLogEntries = liftIO . fmap (first toS . S.decode) . BS.readFile . unRaftLogFile =<< ask
