{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}

module Raft.Metrics
( RaftNodeMetrics(..)
, defaultRaftNodeMetrics
, getMetricsStore
, getRaftNodeMetrics
, setNodeStateLabel
, setLastLogEntryIndexGauge
, setLastLogEntryHashLabel
, setCommitIndexGauge
, incrInvalidCmdCounter
, incrEventsHandledCounter
) where

import Protolude

import qualified Control.Monad.Metrics as Metrics
import qualified Control.Monad.Metrics.Internal as Metrics

import qualified Data.HashMap.Strict as HashMap
import Data.Serialize (Serialize)

import qualified System.Metrics as EKG

import Raft.Log (LastLogEntry, EntryHash(..), hashLastLogEntry, genesisHash)
import Raft.Types (Index, Mode(..))

------------------------------------------------------------------------------
-- Raft Node Metrics
------------------------------------------------------------------------------

data RaftNodeMetrics
  = RaftNodeMetrics
  { invalidCmdCounter :: Int64
  , eventsHandledCounter :: Int64
  , nodeStateLabel :: [Char]
  , lastLogHashLabel :: [Char] -- ^ Base 16 encoded last log entry hash
  , lastLogIndexGauge :: Int64
  , commitIndexGauge :: Int64
  } deriving (Show, Generic, Serialize)

defaultRaftNodeMetrics :: RaftNodeMetrics
defaultRaftNodeMetrics = RaftNodeMetrics
  { invalidCmdCounter = 0
  , eventsHandledCounter = 0
  , nodeStateLabel = "Follower"
  , lastLogHashLabel = toS (unEntryHash genesisHash)
  , lastLogIndexGauge = 0
  , commitIndexGauge = 0
  }

getMetricsStore :: (MonadIO m, Metrics.MonadMetrics m) => m EKG.Store
getMetricsStore = Metrics._metricsStore <$> Metrics.getMetrics

getRaftNodeMetrics :: (MonadIO m, Metrics.MonadMetrics m) => m RaftNodeMetrics
getRaftNodeMetrics = do
  metricsStore <- getMetricsStore
  sample <- liftIO (EKG.sampleAll metricsStore)
  pure RaftNodeMetrics
    { invalidCmdCounter = lookupCounterValue InvalidCmdCounter sample
    , eventsHandledCounter = lookupCounterValue EventsHandledCounter sample
    , nodeStateLabel = toS (lookupLabelValue NodeStateLabel sample)
    , lastLogHashLabel = toS (lookupLastLogEntryHashLabel sample)
    , lastLogIndexGauge = lookupGaugeValue LastLogEntryIndexGauge sample
    , commitIndexGauge = lookupGaugeValue CommitIndexGauge sample
    }

--------------------------------------------------------------------------------
-- Labels
--
--   Labels are variable values and can be used to track e.g. the command line
--   arguments or other free-form values.
--------------------------------------------------------------------------------

data RaftNodeLabel
  = NodeStateLabel
  | LastLogEntryHashLabel
  deriving Show

lookupLabelValue :: RaftNodeLabel -> EKG.Sample -> Text
lookupLabelValue label sample =
  case HashMap.lookup (show label) sample of
    Just (EKG.Label text) -> text
    -- TODO Handle failure in a better way?
    Nothing -> ""
    Just _ -> ""

setRaftNodeLabel :: (MonadIO m, Metrics.MonadMetrics m) => RaftNodeLabel -> Text -> m ()
setRaftNodeLabel label = Metrics.label (show label)

setNodeStateLabel :: (MonadIO m, Metrics.MonadMetrics m) => Mode -> m ()
setNodeStateLabel = setRaftNodeLabel NodeStateLabel . show

setLastLogEntryHashLabel
  :: (MonadIO m, Metrics.MonadMetrics m, Serialize v)
  => LastLogEntry v
  -> m ()
setLastLogEntryHashLabel = setRaftNodeLabel LastLogEntryHashLabel . toS . unEntryHash . hashLastLogEntry

lookupLastLogEntryHashLabel :: EKG.Sample -> Text
lookupLastLogEntryHashLabel sample =
    case HashMap.lookup (show LastLogEntryHashLabel) sample of
      Just (EKG.Label hashAsText) -> hashAsText
      _ -> toS . unEntryHash $ genesisHash

--------------------------------------------------------------------------------
-- Gauges
--
--   Gauges are variable values and can be used to track e.g. the current number
--   of concurrent connections.
--------------------------------------------------------------------------------

data RaftNodeGauge
  = LastLogEntryIndexGauge
  | CommitIndexGauge
  deriving Show

lookupGaugeValue :: RaftNodeGauge -> EKG.Sample -> Int64
lookupGaugeValue gauge sample =
  case HashMap.lookup (show gauge) sample of
    Nothing -> 0
    Just (EKG.Gauge n) -> n
    -- TODO Handle failure in a better way?
    Just _ -> 0

setRaftNodeGauge :: (MonadIO m, Metrics.MonadMetrics m) => RaftNodeGauge -> Int -> m ()
setRaftNodeGauge gauge n = Metrics.gauge (show gauge) n

setLastLogEntryIndexGauge :: (MonadIO m, Metrics.MonadMetrics m) => Index -> m ()
setLastLogEntryIndexGauge = setRaftNodeGauge LastLogEntryIndexGauge . fromIntegral

setCommitIndexGauge :: (MonadIO m, Metrics.MonadMetrics m) => Index -> m ()
setCommitIndexGauge = setRaftNodeGauge CommitIndexGauge . fromIntegral

--------------------------------------------------------------------------------
-- Counters
--
--   Counters are non-negative, monotonically increasing values and can be used
--   to track e.g. the number of requests served since program start.
--------------------------------------------------------------------------------

data RaftNodeCounter
  = InvalidCmdCounter
  | EventsHandledCounter
  deriving Show

lookupCounterValue :: RaftNodeCounter -> EKG.Sample -> Int64
lookupCounterValue counter sample =
  case HashMap.lookup (show counter) sample of
    Nothing -> 0
    Just (EKG.Counter n) -> n
    -- TODO Handle failure in a better way?
    Just _ -> 0

incrRaftNodeCounter :: (MonadIO m, Metrics.MonadMetrics m) => RaftNodeCounter -> m ()
incrRaftNodeCounter = Metrics.increment . show

incrInvalidCmdCounter :: (MonadIO m, Metrics.MonadMetrics m) => m ()
incrInvalidCmdCounter = incrRaftNodeCounter InvalidCmdCounter

incrEventsHandledCounter :: (MonadIO m, Metrics.MonadMetrics m) => m ()
incrEventsHandledCounter = incrRaftNodeCounter EventsHandledCounter
