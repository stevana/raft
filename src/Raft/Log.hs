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

module Raft.Log where

import Protolude

import qualified Crypto.Hash.SHA256 as SHA256

import qualified Data.ByteString as BS
import qualified Data.Map as Map
import Data.Serialize
import Data.Sequence (Seq(..), (|>), foldlWithIndex)

import Raft.Client
import Raft.Types

data EntryIssuer
  = ClientIssuer ClientId SerialNum
  | LeaderIssuer LeaderId
  deriving (Show, Generic, Serialize)

data EntryValue v
  = EntryValue v
  | NoValue -- ^ Used as a first committed entry of a new term
  deriving (Show, Generic, Serialize)

newtype EntryHash = EntryHash ByteString
  deriving (Show, Eq, Generic, Serialize)

genesisHash :: EntryHash
genesisHash = EntryHash $ BS.replicate 32 0

hashEntry :: Serialize v => Entry v -> EntryHash
hashEntry = EntryHash . SHA256.hash . encode

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
  } deriving (Show, Generic, Serialize)

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

type RaftLog m v = (RaftReadLog m v, RaftWriteLog m v, RaftDeleteLog m v)
type RaftLogExceptions m = (Exception (RaftReadLogError m), Exception (RaftWriteLogError m), Exception (RaftDeleteLogError m))

-- | Representation of possible errors that come from reading, writing or
-- deleting logs from the persistent storage
data RaftLogError m where
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
