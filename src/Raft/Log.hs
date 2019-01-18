{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE GADTs #-}

module Raft.Log (

  EntryIssuer(..),
  EntryValue(..),

  EntryHash,
  genesisHash,
  hashEntry,

  Entry(..),
  Entries,

  RaftInitLog(..),
  ReadEntriesSpec(..),
  ReadEntriesRes(..),
  IndexInterval(..),
  RaftReadLog(..),
  RaftWriteLog(..),
  RaftDeleteLog(..),
  DeleteSuccess(..),

  RaftLog(..),
  RaftLogError,
  RaftLogExceptions,

  updateLog,
  clientReqData,
  readEntries,

) where

import Protolude

import qualified Crypto.Hash.SHA256 as SHA256

import qualified Data.ByteString as BS
import qualified Data.ByteString.Base16 as BS16
import qualified Data.Map as Map
import Data.Serialize
import Data.Sequence (Seq(..), (|>), foldlWithIndex)
import qualified Data.Sequence as Seq

import Database.PostgreSQL.Simple.ToField (Action(..), ToField(..))
import Database.PostgreSQL.Simple.FromField (FromField(..), returnError, ResultError(..))

import Raft.Types

data EntryIssuer
  = ClientIssuer ClientId SerialNum
  | LeaderIssuer LeaderId
  deriving (Show, Read, Eq, Generic, Serialize)

data EntryValue v
  = EntryValue v
  | NoValue -- ^ Used as a first committed entry of a new term
  deriving (Show, Eq, Generic, Serialize)

newtype EntryHash = EntryHash ByteString
  deriving (Show, Eq, Ord, Generic, Serialize)

genesisHash :: EntryHash
genesisHash = EntryHash $ BS16.encode $ BS.replicate 32 0

hashEntry :: Serialize v => Entry v -> EntryHash
hashEntry = EntryHash . BS16.encode . SHA256.hash . encode

-- | Representation of an entry in the replicated log
data Entry v = Entry
  { entryIndex :: Index
    -- ^ Index of entry in the log
  , entryTerm :: Term
    -- ^ Term when entry was received by leader
  , entryValue :: EntryValue v
    -- ^ Command to update state machine
  , entryIssuer :: EntryIssuer
    -- ^ Id of the client that issued the command
  , entryPrevHash :: EntryHash
  } deriving (Show, Eq, Generic, Serialize)

type Entries v = Seq (Entry v)

data InvalidLog
  = InvalidIndex { expectedIndex :: Index, actualIndex :: Index }
  | InvalidPrevHash { expectedHash :: EntryHash, actualHash :: EntryHash }
  deriving (Show)

-- | For debugging & testing purposes
validateLog :: (Serialize v) => Entries v -> Either InvalidLog ()
validateLog es =
    case es of
      Empty -> Right ()
      e :<| _ ->
        second (const ()) $
          foldlWithIndex accValidateEntry (Right Nothing) es
  where
    accValidateEntry (Left err) _ _ = Left err
    accValidateEntry (Right mPrevEntry) idx e = validateEntry mPrevEntry idx e

    validateEntry mPrevEntry expectedIdx currEntry = do
        case mPrevEntry of
          Nothing -> validatePrevHash genesisHash currEntryPrevHash
          Just prevEntry -> validatePrevHash (hashEntry prevEntry) currEntryPrevHash
        validateIndex expectedEntryIdx currEntryIdx
        pure (Just currEntry)
      where
        currEntryIdx = entryIndex currEntry
        expectedEntryIdx = Index (fromIntegral expectedIdx + 1)

        currEntryPrevHash = entryPrevHash currEntry

    validateIndex :: Index -> Index -> Either InvalidLog ()
    validateIndex  expectedIndex currIndex
      | expectedIndex /= currIndex =
          Left (InvalidIndex expectedIndex currIndex)
      | otherwise = Right ()

    validatePrevHash :: EntryHash -> EntryHash -> Either InvalidLog ()
    validatePrevHash expectedHash currHash
      | expectedHash /= currHash =
          Left (InvalidPrevHash expectedHash currHash)
      | otherwise = Right ()

clientReqData :: Entries v -> Map ClientId (SerialNum, Index)
clientReqData = go mempty
  where
    go acc es =
      case es of
        Empty -> acc
        e :<| rest ->
          case entryIssuer e of
            LeaderIssuer _ -> go acc rest
            ClientIssuer cid sn -> go (Map.insert cid (sn, entryIndex e) acc) rest

-- | Provides an interface to initialize a fresh log entry storage
class RaftInitLog m v where
  type RaftInitLogError m
  initializeLog :: Proxy v -> m (Either (RaftInitLogError m) ())

-- | Provides an interface for nodes to write log entries to storage.
class (Show (RaftWriteLogError m), Monad m) => RaftWriteLog m v where
  type RaftWriteLogError m
  -- | Write the given log entries to storage
  writeLogEntries
    :: Exception (RaftWriteLogError m)
    => Entries v -> m (Either (RaftWriteLogError m) ())

data DeleteSuccess v = DeleteSuccess

-- | Provides an interface for nodes to delete log entries from storage.
class (Show (RaftDeleteLogError m), Monad m) => RaftDeleteLog m v where
  type RaftDeleteLogError m
  -- | Delete log entries from a given index; e.g. 'deleteLogEntriesFrom 7'
  -- should delete every log entry with an index >= 7.
  deleteLogEntriesFrom
    :: Exception (RaftDeleteLogError m)
    => Index -> m (Either (RaftDeleteLogError m) (DeleteSuccess v))

-- | Provides an interface for nodes to read log entries from storage.
class (Show (RaftReadLogError m), Monad m) => RaftReadLog m v where
  type RaftReadLogError m
  -- | Read the log at a given index
  readLogEntry
    :: Exception (RaftReadLogError m)
    => Index -> m (Either (RaftReadLogError m) (Maybe (Entry v)))
  -- | Read log entries from a specific index onwards, including the specific
  -- index
  readLogEntriesFrom
    :: Exception (RaftReadLogError m)
    => Index -> m (Either (RaftReadLogError m) (Entries v))
  -- | Read the last log entry in the log
  readLastLogEntry
    :: Exception (RaftReadLogError m)
    => m (Either (RaftReadLogError m) (Maybe (Entry v)))

  default readLogEntriesFrom
    :: Exception (RaftReadLogError m)
    => Index
    -> m (Either (RaftReadLogError m) (Entries v))
  readLogEntriesFrom idx = do
      eLastLogEntry <- readLastLogEntry
      case eLastLogEntry of
        Left err -> pure (Left err)
        Right Nothing -> pure (Right Empty)
        Right (Just lastLogEntry)
          | entryIndex lastLogEntry < idx -> pure (Right Empty)
          | otherwise -> fmap (|> lastLogEntry) <$> go (decrIndexWithDefault0 (entryIndex lastLogEntry))
    where
      go idx'
        | idx' < idx || idx' == 0 = pure (Right Empty)
        | otherwise = do
            eLogEntry <- readLogEntry idx'
            case eLogEntry of
              Left err -> pure (Left err)
              Right Nothing -> panic "Malformed log"
              Right (Just logEntry) -> fmap (|> logEntry) <$>  go (decrIndexWithDefault0 idx')

type RaftLog m v = (RaftInitLog m v, RaftReadLog m v, RaftWriteLog m v, RaftDeleteLog m v)
type RaftLogExceptions m = (Exception (RaftInitLogError m), Exception (RaftReadLogError m), Exception (RaftWriteLogError m), Exception (RaftDeleteLogError m))

-- | Representation of possible errors that come from reading, writing or
-- deleting logs from the persistent storage
data RaftLogError m where
  RaftLogInitError :: Show (RaftInitLogError m) => RaftInitLogError m -> RaftLogError m
  RaftLogReadError :: Show (RaftReadLogError m) => RaftReadLogError m -> RaftLogError m
  RaftLogWriteError :: Show (RaftWriteLogError m) => RaftWriteLogError m -> RaftLogError m
  RaftLogDeleteError :: Show (RaftDeleteLogError m) => RaftDeleteLogError m -> RaftLogError m

deriving instance Show (RaftLogError m)

updateLog
  :: forall m v.
     ( RaftDeleteLog m v, Exception (RaftDeleteLogError m)
     , RaftWriteLog m v, Exception (RaftWriteLogError m)
     )
  => Entries v
  -> m (Either (RaftLogError m) ())
updateLog entries =
  case entries of
    Empty -> pure (Right ())
    e :<| _ -> do
      eDel <- deleteLogEntriesFrom @m @v (entryIndex e)
      case eDel of
        Left err -> pure (Left (RaftLogDeleteError err))
        Right DeleteSuccess -> first RaftLogWriteError <$> writeLogEntries entries

--------------------------------------------------------------------------------
-- Reading entries by <X>
--
-- Note:
--
--   This would be best left up to the implementor, instead of
--   'readEntriesFrom', it would be best to force the programmer to specify a
--   `readEntriesBetween` function that reads entries from disk between a
--   specified interval. For efficiency and ease of use, it might be best to
--   provide an instance of a typeclass from a popular haskell SQL lib for the
--   'Entry v' type so that reading entries by index _or_ hash is efficient out
--   of the box.
--
--------------------------------------------------------------------------------

data IndexInterval = IndexInterval (Maybe Index) (Maybe Index)
  deriving (Show, Generic, Serialize)

data ReadEntriesSpec
  = ByIndex Index
  | ByIndices IndexInterval
  deriving (Show, Generic, Serialize)

data ReadEntriesError m where
  EntryDoesNotExist :: Either EntryHash Index -> ReadEntriesError m
  InvalidIntervalSpecified :: (Index, Index) -> ReadEntriesError m
  ReadEntriesError :: Exception (RaftReadLogError m) => RaftReadLogError m -> ReadEntriesError m

deriving instance Show (ReadEntriesError m)
deriving instance Typeable m => Exception (ReadEntriesError m)

-- | The result of reading one or more
data ReadEntriesRes v
  = OneEntry (Entry v)
  | ManyEntries (Entries v)

readEntries
  :: forall m v. (RaftReadLog m v, Exception (RaftReadLogError m))
  => ReadEntriesSpec
  -> m (Either (ReadEntriesError m) (ReadEntriesRes v))
readEntries res =
  case res of
    ByIndex idx -> do
      res <- readLogEntry idx
      case res of
        Left err -> pure (Left (ReadEntriesError err))
        Right Nothing -> pure (Left (EntryDoesNotExist (Right idx)))
        Right (Just e) -> pure (Right (OneEntry e))
    ByIndices interval -> fmap ManyEntries <$> readEntriesByIndices interval

-- | Read entries from the log between two indices
readEntriesByIndices
  :: forall m v. (RaftReadLog m v, Exception (RaftReadLogError m))
  => IndexInterval
  -> m (Either (ReadEntriesError m) (Entries v))
readEntriesByIndices (IndexInterval l h) =
  case (l,h) of
    (Nothing, Nothing) ->
      first ReadEntriesError <$> readLogEntriesFrom (Index 0)
    (Nothing, Just hidx) ->
      bimap ReadEntriesError (Seq.takeWhileL ((<= hidx) . entryIndex))
        <$> readLogEntriesFrom (Index 0)
    (Just lidx, Nothing) ->
      first ReadEntriesError <$> readLogEntriesFrom lidx
    (Just lidx, Just hidx)
      | lidx >= hidx ->
          pure (Left (InvalidIntervalSpecified (lidx, hidx)))
      | otherwise ->
          bimap ReadEntriesError (Seq.takeWhileL ((<= hidx) . entryIndex))
            <$> readLogEntriesFrom lidx

--------------------------------------------------------------------------------
-- PostgreSQL Utils
--------------------------------------------------------------------------------

instance Serialize v => ToField (EntryValue v) where
  toField = EscapeByteA . encode

instance (Typeable v, Serialize v) => FromField (EntryValue v) where
  fromField f mdata = do
    bs <- fromField f mdata
    case decode <$> bs of
      Nothing          -> returnError UnexpectedNull f ""
      Just (Left err)  -> returnError ConversionFailed f err
      Just (Right entry) -> return entry

-- TODO don't use show/read
instance ToField EntryIssuer where
  toField entryIssuer = Escape (show entryIssuer)

-- TODO don't use show/read
instance FromField EntryIssuer where
  fromField f mdata = do
    case readEither . toS <$> mdata of
      Nothing -> returnError UnexpectedNull f ""
      Just (Left err) -> returnError ConversionFailed f err
      Just (Right entryIssuer) -> return entryIssuer

instance ToField EntryHash where
  toField (EntryHash hbs) = toField hbs

instance FromField EntryHash where
  fromField f = fmap EntryHash . fromField f
