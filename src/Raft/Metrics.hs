{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}

module Raft.Metrics
( RaftNodeMetrics
, getMetricsStore
, getRaftNodeMetrics
, setRaftNodeStateLabel
, incrInvalidCmdCounter
, incrEventsHandledCounter
) where

import Protolude

import qualified Control.Monad.Metrics as Metrics
import qualified Control.Monad.Metrics.Internal as Metrics

import qualified Data.HashMap.Strict as HashMap
import Data.Serialize (Serialize)

import qualified System.Metrics as EKG

import Raft.Types (Mode(..))

------------------------------------------------------------------------------
-- Raft Node Metrics
------------------------------------------------------------------------------

data RaftNodeMetrics
  = RaftNodeMetrics
  { invalidCmdCounter :: Int64
  , eventsHandledCounter :: Int64
  , nodeStateLabel :: [Char]
  } deriving (Show, Generic, Serialize)

getMetricsStore :: (MonadIO m, Metrics.MonadMetrics m) => m EKG.Store
getMetricsStore = Metrics._metricsStore <$> Metrics.getMetrics

getRaftNodeMetrics :: (MonadIO m, Metrics.MonadMetrics m) => m RaftNodeMetrics
getRaftNodeMetrics = do
  metricsStore <- getMetricsStore
  sample <- liftIO (EKG.sampleAll metricsStore)
  pure RaftNodeMetrics
    { invalidCmdCounter = lookupCounterValue InvalidCmdCounter sample
    , eventsHandledCounter = lookupCounterValue EventsHandledCounter sample
    , nodeStateLabel = toS (lookupLabelValue RaftNodeStateLabel sample)
    }

--------------------------------------------------------------------------------
-- Labels
--------------------------------------------------------------------------------

data RaftNodeLabel
  = RaftNodeStateLabel
  deriving Show

lookupLabelValue :: RaftNodeLabel -> EKG.Sample -> Text
lookupLabelValue label sample =
  case HashMap.lookup (show label) sample of
    Just (EKG.Label text) -> text
    -- TODO Handle failure in a better way?
    Nothing -> ""
    Just _ -> ""

setRaftNodeLabel :: (MonadIO m, Metrics.MonadMetrics m) => RaftNodeLabel -> Mode -> m ()
setRaftNodeLabel label = Metrics.label (show label) . show

setRaftNodeStateLabel :: (MonadIO m, Metrics.MonadMetrics m) => Mode -> m ()
setRaftNodeStateLabel = setRaftNodeLabel RaftNodeStateLabel

--------------------------------------------------------------------------------
-- Counters
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
