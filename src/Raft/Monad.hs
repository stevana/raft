{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeFamilyDependencies #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# Language ConstraintKinds #-}
{-# LANGUAGE AllowAmbiguousTypes #-}

module Raft.Monad (

  MonadRaft
, MonadRaftChan(..)

, RaftThreadRole(..)
, MonadRaftFork(..)

, RaftEnv(..)
, initializeRaftEnv
, RaftT
, runRaftT

, writeRaftEventChan
, readRaftEventChan


, Raft.Monad.logInfo
, Raft.Monad.logDebug
, Raft.Monad.logCritical
, Raft.Monad.logAndPanic

) where

import Protolude hiding (STM, TChan, readTChan, writeTChan, newTChan, atomically)

import qualified Control.Monad.Metrics as Metrics
import Control.Monad.Catch
import Control.Monad.Fail
import Control.Monad.Trans.Class
import qualified Control.Monad.Conc.Class as Conc

import Control.Concurrent.Classy.STM.TChan

import Raft.Config
import Raft.Event
import Raft.Logging
import Raft.Metrics (incrEventsReceivedCounter, incrEventsHandledCounter)
import Raft.NodeState

import Test.DejaFu.Conc (ConcIO)
import qualified Test.DejaFu.Types as TDT

--------------------------------------------------------------------------------
-- Raft Monad Class
--------------------------------------------------------------------------------

type MonadRaft v m = (MonadRaftChan v m, MonadRaftFork m)

-- | The typeclass specifying the datatype used as the core event channel in the
-- main raft event loop, as well as functions for creating, reading, and writing
-- to the channel, and how to fork a computation that performs some action with
-- the channel.
--
-- Note: This module uses AllowAmbiguousTypes which removes the necessity for
-- Proxy value arguments in lieu of TypeApplication. For example:
--
-- @
--   newRaftChan @v
-- @
--
-- instead of
--
-- @
--   newRaftChan (Proxy :: Proxy v)
-- @
class Monad m => MonadRaftChan v m where
  type RaftEventChan v m
  readRaftChan :: RaftEventChan v m -> m (Event v)
  writeRaftChan :: RaftEventChan v m -> Event v -> m ()
  newRaftChan :: m (RaftEventChan v m)

instance MonadRaftChan v IO where
  type RaftEventChan v IO = TChan (Conc.STM IO) (Event v)
  readRaftChan = Conc.atomically . readTChan
  writeRaftChan chan = Conc.atomically . writeTChan chan
  newRaftChan = Conc.atomically newTChan

instance MonadRaftChan v ConcIO where
  type RaftEventChan v ConcIO = TChan (Conc.STM ConcIO) (Event v)
  readRaftChan = Conc.atomically . readTChan
  writeRaftChan chan = Conc.atomically . writeTChan chan
  newRaftChan = Conc.atomically newTChan

data RaftThreadRole
  = RPCHandler
  | ClientRequestHandler
  | CustomThreadRole Text
  deriving Show

-- | The typeclass encapsulating the concurrency operations necessary for the
-- implementation of the main event handling loop.
class Monad m => MonadRaftFork m where
  type RaftThreadId m
  raftFork
    :: RaftThreadRole -- ^ The role of the current thread being forked
    -> m ()   -- ^ The action to fork
    -> m (RaftThreadId m)

instance MonadRaftFork IO where
  type RaftThreadId IO = Protolude.ThreadId
  raftFork _ = forkIO

instance MonadRaftFork ConcIO where
  type RaftThreadId ConcIO = TDT.ThreadId
  raftFork r = Conc.forkN (show r)

--------------------------------------------------------------------------------
-- Raft Monad
--------------------------------------------------------------------------------

-- | The raft server environment composed of the concurrent variables used in
-- the effectful raft layer.
data RaftEnv sm v m = RaftEnv
  { eventChan :: RaftEventChan v m
  , resetElectionTimer :: m ()
  , resetHeartbeatTimer :: m ()
  , raftNodeConfig :: RaftNodeConfig
  , raftNodeLogCtx :: LogCtx (RaftT sm v m)
  , raftNodeMetrics :: Metrics.Metrics
  }

newtype RaftT sm v m a = RaftT
  { unRaftT :: ReaderT (RaftEnv sm v m) (StateT (RaftNodeState sm v) m) a
  } deriving newtype (Functor, Applicative, Monad, MonadReader (RaftEnv sm v m), MonadState (RaftNodeState sm v), MonadFail, Alternative, MonadPlus)

instance MonadTrans (RaftT sm v) where
  lift = RaftT . lift . lift

deriving newtype instance MonadIO m => MonadIO (RaftT sm v m)
deriving newtype instance MonadThrow m => MonadThrow (RaftT sm v m)
deriving newtype instance MonadCatch m => MonadCatch (RaftT sm v m)
deriving newtype instance MonadMask m => MonadMask (RaftT sm v m)

instance MonadRaftFork m => MonadRaftFork (RaftT sm v m) where
  type RaftThreadId (RaftT sm v m) = RaftThreadId m
  raftFork s m = do
    raftEnv <- ask
    raftState <- get
    lift $ raftFork s (runRaftT raftState raftEnv m)

instance Monad m => RaftLogger sm v (RaftT sm v m) where
  loggerCtx = do
    raftNodeState :: RaftNodeState sm v <- get
    (,) <$> asks (raftConfigNodeId . raftNodeConfig) <*> pure raftNodeState

instance Monad m => Metrics.MonadMetrics (RaftT sm v m) where
  getMetrics = asks raftNodeMetrics

initializeRaftEnv
  :: MonadIO m
  => RaftEventChan v m
  -> m ()
  -> m ()
  -> RaftNodeConfig
  -> LogCtx (RaftT sm v m)
  -> m (RaftEnv sm v m)
initializeRaftEnv eventChan resetElectionTimer resetHeartbeatTimer nodeConfig logCtx = do
  metrics <- liftIO Metrics.initialize
  pure RaftEnv
    { eventChan = eventChan
    , resetElectionTimer = resetElectionTimer
    , resetHeartbeatTimer = resetHeartbeatTimer
    , raftNodeConfig = nodeConfig
    , raftNodeLogCtx = logCtx
    , raftNodeMetrics = metrics
    }

runRaftT
  :: Monad m
  => RaftNodeState sm v
  -> RaftEnv sm v m
  -> RaftT sm v m a
  -> m a
runRaftT raftNodeState raftEnv =
  flip evalStateT raftNodeState . flip runReaderT raftEnv . unRaftT

writeRaftEventChan :: forall sm v m. (MonadIO m, MonadRaftChan v m) => Event v -> RaftT sm v m ()
writeRaftEventChan e = do
  incrEventsReceivedCounter
  eventChan <- asks eventChan
  lift (writeRaftChan eventChan e)

readRaftEventChan :: forall sm v m. (MonadIO m, MonadRaftChan v m) => RaftT sm v m (Event v)
readRaftEventChan = do
  incrEventsHandledCounter
  eventChan <- asks eventChan
  lift (readRaftChan eventChan)

------------------------------------------------------------------------------
-- Logging
------------------------------------------------------------------------------

logInfo :: MonadIO m => Text -> RaftT sm v m ()
logInfo msg = flip logInfoIO msg =<< asks raftNodeLogCtx

logDebug :: MonadIO m => Text -> RaftT sm v m ()
logDebug msg = flip logDebugIO msg =<< asks raftNodeLogCtx

logCritical :: MonadIO m => Text -> RaftT sm v m ()
logCritical msg = flip logCriticalIO msg =<< asks raftNodeLogCtx

logAndPanic :: MonadIO m => Text -> RaftT sm v m a
logAndPanic msg = flip logAndPanicIO msg =<< asks raftNodeLogCtx
