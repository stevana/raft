{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE TemplateHaskell #-}

module Raft.Log.PostgreSQL (
  runRaftPostgresT,
  runRaftPostgresM,

  raftDatabaseName,
  raftDatabaseConnInfo,
  initConnInfo,

  setupDB,
  deleteDB

) where

import Protolude hiding (atomically, try, bracket, catches, tryReadTMVar, newEmptyTMVar, STM, Handler)

import Control.Concurrent.Classy (STM, MonadConc, atomically)
import Control.Concurrent.Classy.STM.TMVar
import Control.Monad.Catch
import Control.Monad.Fail
import Control.Monad.Trans.Class

import Data.FileEmbed
import qualified Data.Sequence as Seq
import Data.Serialize (Serialize)

import Database.PostgreSQL.Simple
import Database.PostgreSQL.Simple.Types (Identifier(..))

import Raft.Log
import Raft.Types

data RaftPostgresEnv = RaftPostgresEnv
  { raftPostgresConnInfo :: ConnectInfo
  , raftPostgresConn :: TMVar (STM IO) Connection
  }

-- | A single threaded PostgreSQL storage monad transformer
newtype RaftPostgresT m a = RaftPostgresT { unRaftPostgresT :: ReaderT RaftPostgresEnv m a }
  deriving newtype (Functor, Applicative, Monad, MonadIO, MonadFail, MonadReader RaftPostgresEnv, Alternative, MonadPlus, MonadTrans)

deriving newtype instance MonadThrow m => MonadThrow (RaftPostgresT m)
deriving newtype instance MonadCatch m => MonadCatch (RaftPostgresT m)
deriving newtype instance MonadMask m => MonadMask (RaftPostgresT m)
deriving newtype instance MonadConc m => MonadConc (RaftPostgresT m)

runRaftPostgresT :: MonadIO m => ConnectInfo -> RaftPostgresT m a -> m a
runRaftPostgresT connInfo m = do
  connTMVar <- liftIO $ atomically newEmptyTMVar
  let postgresEnv = RaftPostgresEnv connInfo connTMVar
  runReaderT (unRaftPostgresT m) postgresEnv

type RaftPostgresM = RaftPostgresT IO

runRaftPostgresM :: ConnectInfo -> RaftPostgresM a -> IO a
runRaftPostgresM = runRaftPostgresT

data RaftPostgresError
  = RaftPostgresError PGError
  | RaftPostgresFailedToConnect PGError
  | RaftPostgresSerializeError Text
  deriving (Show)

instance Exception (RaftPostgresError)

-- | Helper function for RaftXLog typeclasses such that we can easily catch all
-- @PGError@s and connect to the DB if we haven't already
withRaftPostgresConn
  :: (MonadIO m, MonadConc m)
  => (Connection -> IO (Either RaftPostgresError a))
  -> RaftPostgresT m (Either RaftPostgresError a)
withRaftPostgresConn f = do
  mconn <- liftIO . atomically . tryReadTMVar =<< asks raftPostgresConn
  eConn <-
    case mconn of
      -- If there is no existing connection, attempt to connect and store the
      -- connection in the TMVar
      Nothing -> do
        eConn <- liftIO . tryPG . connect =<< asks raftPostgresConnInfo
        case eConn of
          Left err -> pure (Left (RaftPostgresFailedToConnect err))
          Right conn -> do
            connTMVar <- asks raftPostgresConn
            liftIO . atomically $ putTMVar connTMVar conn
            pure (Right conn)
      Just conn -> pure (Right conn)
  case eConn of
    Left err -> pure (Left err)
    Right conn -> do
      eRes <- liftIO (tryPG (f conn))
      case eRes of
        Left err -> pure (Left (RaftPostgresError err))
        Right (Left err) -> pure (Left err)
        Right (Right a) -> pure (Right a)

instance (MonadIO m) => RaftInitLog (RaftPostgresT m) v where
  type RaftInitLogError (RaftPostgresT m) = RaftPostgresError
  initializeLog _ = do
    eConn <- liftIO . setupDB =<< asks raftPostgresConnInfo
    case eConn of
      Left err -> pure (Left (RaftPostgresError err))
      Right conn -> do
        connTMVar <- asks raftPostgresConn
        fmap Right . liftIO . atomically $ putTMVar connTMVar conn

instance (Typeable v, Serialize v, MonadIO m, MonadConc m) => RaftReadLog (RaftPostgresT m) v where
  type RaftReadLogError (RaftPostgresT m) = RaftPostgresError
  readLogEntry idx =
    withRaftPostgresConn $ \conn ->
      Right . fmap rowTypeToEntry . listToMaybe <$>
        query conn "select * from entries where entryIndex = ?" (Only idx)

  readLogEntriesFrom idx =
    withRaftPostgresConn $ \conn ->
      Right . Seq.fromList . map rowTypeToEntry <$>
        query conn "select * from entries where entryIndex >= ?" (Only idx)

  readLastLogEntry =
    withRaftPostgresConn $ \conn ->
      Right . fmap rowTypeToEntry . listToMaybe <$>
        query_ conn "SELECT * FROM entries ORDER BY entryIndex DESC LIMIT 1"

instance (Serialize v, MonadIO m, MonadConc m) => RaftWriteLog (RaftPostgresT m) v where
  type RaftWriteLogError (RaftPostgresT m) = RaftPostgresError
  writeLogEntries entries =
    withRaftPostgresConn $ \conn ->
      fmap Right . void $
        executeMany conn "INSERT INTO entries VALUES (?,?,?,?,?,?)" (map entryToRowType (toList entries))

instance (Serialize v, MonadIO m, MonadConc m) => RaftDeleteLog (RaftPostgresT m) v where
  type RaftDeleteLogError (RaftPostgresT m) = RaftPostgresError
  deleteLogEntriesFrom idx =
    withRaftPostgresConn $ \conn ->
      fmap (const $ Right DeleteSuccess) . void $
        execute conn "DELETE FROM entries WHERE entryIndex >= ?" (Only idx)

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------

data EntryRow v = EntryRow
  { entryRowIndex :: Index
  , entryRowTerm :: Term
  , entryRowValueHash :: EntryHash
  , entryRowValue :: EntryValue v
  , entryRowIssuer :: EntryIssuer
  , entryRowPrevHash :: EntryHash
  } deriving (Show, Generic, ToRow, FromRow)

entryToRowType :: Serialize v => Entry v -> EntryRow v
entryToRowType entry@Entry{..} =
  EntryRow
    { entryRowIndex = entryIndex
    , entryRowTerm = entryTerm
    , entryRowValueHash = hashEntry entry
    , entryRowValue = entryValue
    , entryRowIssuer = entryIssuer
    , entryRowPrevHash = entryPrevHash
    }

rowTypeToEntry :: Serialize v => EntryRow v -> Entry v
rowTypeToEntry EntryRow{..} = Entry
  { entryIndex = entryRowIndex
  , entryTerm = entryRowTerm
  , entryValue = entryRowValue
  , entryIssuer = entryRowIssuer
  , entryPrevHash = entryRowPrevHash
  }

--------------------------------------------------------------------------------
-- Create Database for Entry Values
--------------------------------------------------------------------------------

data PGError
  = PGSqlError SqlError
  | PGFormatError FormatError
  | PGQueryError QueryError
  | PGResultError ResultError
  | PGUnexpectedError Text
  deriving (Show)

raftDatabasePrefix :: [Char]
raftDatabasePrefix = "libraft_"

raftDatabaseName :: [Char] -> [Char]
raftDatabaseName suffix = raftDatabasePrefix ++ suffix

raftDatabaseConnInfo :: [Char] -> [Char] -> [Char] -> ConnectInfo
raftDatabaseConnInfo usrnm pswd dbSuffix =
  defaultConnectInfo {
      connectUser = usrnm
    , connectPassword = pswd
    , connectDatabase = raftDatabaseName dbSuffix
    }

initConnInfo :: ConnectInfo
initConnInfo = defaultConnectInfo
  { connectDatabase = "postgres"
  , connectUser = "libraft_test"
  , connectPassword = "libraft_test"
  }

tryPG :: IO a -> IO (Either PGError a)
tryPG action =
    catches (Right <$> action)
      [ catchSqlError, catchFmtError, catchQueryError, catchResultError, catchAllError ]
  where
    handler :: Exception e => (e -> IO a) -> Handler IO a
    handler = Handler

    catchSqlError    = handler (pure . Left . PGSqlError)
    catchFmtError    = handler (pure . Left . PGFormatError)
    catchQueryError  = handler (pure . Left . PGQueryError)
    catchResultError = handler (pure . Left . PGResultError)
    catchAllError    = handler (pure . Left . PGUnexpectedError . (show :: SomeException -> Text))

createDB :: [Char] -> Connection -> IO Int64
createDB dbName conn =
  execute conn "CREATE DATABASE ?" (Only $ Identifier (toS dbName))

deleteDB :: [Char] -> Connection -> IO Int64
deleteDB dbName conn =
  execute conn "DROP DATABASE IF EXISTS ?" (Only $ Identifier (toS dbName))

-- | Load the raft schema at compile time using TH
raftSchema :: IsString a => a
raftSchema = $(makeRelativeToProject "postgres/entries.sql" >>= embedStringFile)

-- | Execute DB schema to build the raft database
createEntriesTable :: Connection -> IO Int64
createEntriesTable conn = execute_ conn raftSchema

-- | Create the libraft database to store all log entries
setupDB :: ConnectInfo -> IO (Either PGError Connection)
setupDB connInfo = tryPG $ do
    -- Create the database with the "postgres" user
    bracket (connect initConnInfo) close $ \conn ->
      createDB dbName conn
    -- Connect to the DB with the provided connection info
    conn <- connect connInfo
    -- Create the log entries table with the resulting connection
    createEntriesTable conn
    pure conn
  where

    dbName = connectDatabase connInfo
