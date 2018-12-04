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
{-# LANGUAGE FunctionalDependencies #-}

module Examples.Raft.FileStore where

import Protolude

import Control.Concurrent.Classy hiding (catch, ThreadId)
import Control.Monad.Fail
import Control.Monad.Catch
import Control.Monad.Trans.Class

import qualified Data.ByteString as BS
import Data.Sequence ((><))
import qualified Data.Sequence as Seq
import qualified Data.Serialize as S

import Raft

newtype NodeEnvError = NodeEnvError Text
  deriving (Show)

instance Exception NodeEnvError

data NodeFileStoreEnv = NodeFileStoreEnv
  { nfsPersistentState :: FilePath
  , nfsLogEntries :: FilePath
  }

newtype RaftFileStoreT m a = RaftFileStoreT { unRaftFileStoreT :: ReaderT NodeFileStoreEnv m a }
  deriving (Functor, Applicative, Monad, MonadIO, MonadFail, MonadReader NodeFileStoreEnv, Alternative, MonadPlus, MonadTrans)

deriving instance MonadConc m => MonadThrow (RaftFileStoreT m)
deriving instance MonadConc m => MonadCatch (RaftFileStoreT m)
deriving instance MonadConc m => MonadMask (RaftFileStoreT m)
deriving instance MonadConc m => MonadConc (RaftFileStoreT m)

--------------------
-- Raft Instances --
--------------------

instance (MonadIO m, MonadConc m, S.Serialize v) => RaftWriteLog (RaftFileStoreT m) v where
  type RaftWriteLogError (RaftFileStoreT m) = NodeEnvError
  writeLogEntries newEntries = do
    entriesPath <- asks nfsLogEntries
    eLogEntries <- readLogEntries
    case eLogEntries of
      Left err -> panic ("writeLogEntries: " <> err)
      Right currEntries -> liftIO $ Right <$> BS.writeFile entriesPath (S.encode (currEntries >< newEntries))

instance (MonadIO m, MonadConc m) => RaftPersist (RaftFileStoreT m) where
  type RaftPersistError (RaftFileStoreT m) = NodeEnvError
  writePersistentState ps = do
    psPath <- asks nfsPersistentState
    liftIO $ Right <$> BS.writeFile psPath (S.encode ps)

  readPersistentState = do
    psPath <- asks nfsPersistentState
    fileContent <- liftIO $ BS.readFile psPath
    case S.decode fileContent of
      Left err -> panic (toS $ "readPersistentState: " ++ err)
      Right ps -> pure $ Right ps

instance (MonadIO m, MonadConc m, S.Serialize v) => RaftReadLog (RaftFileStoreT m) v where
  type RaftReadLogError (RaftFileStoreT m) = NodeEnvError
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
      Left err -> panic (toS err)
      Right entries -> case entries of
        Seq.Empty -> pure (Right Nothing)
        (_ Seq.:|> e) -> pure (Right (Just e))

instance (MonadIO m, MonadConc m, S.Serialize v) => RaftDeleteLog (RaftFileStoreT m) v where
  type RaftDeleteLogError (RaftFileStoreT m) = NodeEnvError
  deleteLogEntriesFrom idx = do
    eLogEntries <- readLogEntries
    case eLogEntries of
      Left err -> panic ("deleteLogEntriesFrom: " <> err)
      Right (entries :: Entries v) -> do
        let newLogEntries = Seq.dropWhileR ((>= idx) . entryIndex) entries
        entriesPath <- asks nfsLogEntries
        liftIO $ BS.writeFile entriesPath (S.encode newLogEntries)
        pure (Right DeleteSuccess)

readLogEntries :: (MonadIO m, S.Serialize v) => RaftFileStoreT m (Either Text (Entries v))
readLogEntries = liftIO . fmap (first toS . S.decode) . BS.readFile . toS =<< asks nfsLogEntries
