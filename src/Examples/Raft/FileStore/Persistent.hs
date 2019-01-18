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

module Examples.Raft.FileStore.Persistent where

import Protolude hiding (try)

import Control.Concurrent.Classy hiding (catch, ThreadId)
import Control.Monad.Fail
import Control.Monad.Catch
import Control.Monad.Trans.Class

import qualified Data.ByteString as BS
import qualified Data.Serialize as S

import System.Directory (doesFileExist)
import qualified System.AtomicWrite.Writer.ByteString as AW

import Raft.Persistent

newtype RaftPersistFileStoreError = RaftPersistFileStoreError Text
  deriving (Show)

newtype RaftPersistFile = RaftPersistFile FilePath

instance Exception RaftPersistFileStoreError

newtype RaftPersistFileStoreT m a = RaftPersistFileStoreT { unRaftPersistFileStoreT :: ReaderT RaftPersistFile m a }
  deriving (Functor, Applicative, Monad, MonadIO, MonadFail, MonadReader RaftPersistFile, Alternative, MonadPlus, MonadTrans)

deriving instance MonadThrow m => MonadThrow (RaftPersistFileStoreT m)
deriving instance MonadCatch m => MonadCatch (RaftPersistFileStoreT m)
deriving instance MonadMask m => MonadMask (RaftPersistFileStoreT m)
deriving instance MonadConc m => MonadConc (RaftPersistFileStoreT m)

--------------------------------------------------------------------------------
-- Raft Instances
--------------------------------------------------------------------------------

-- A RaftPersist instance for the RaftPersistFileStoreT monad.
--
-- Warning: `atomicWriteFile` causes large pauses on the order of hundreds of
-- milliseconds. This can cause leadership to change since the node become
-- unresponsive for much longer than the recommended election timeouts.
instance (MonadIO m, MonadConc m) => RaftPersist (RaftPersistFileStoreT m) where
  type RaftPersistError (RaftPersistFileStoreT m) = RaftPersistFileStoreError

  initializePersistentState = do
    RaftPersistFile psFile <- ask
    fileExists <- liftIO $ doesFileExist psFile
    if fileExists
      then pure $ Left (RaftPersistFileStoreError ("Persistent file " <> toS psFile <> " already exists!"))
      else do
        eRes <-
          liftIO . try $
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
