{-# LANGUAGE StandaloneDeriving #-}
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
data RaftEnv v m = RaftEnv
  { eventChan :: RaftEventChan v m
  , resetElectionTimer :: m ()
  , resetHeartbeatTimer :: m ()
  , raftNodeConfig :: RaftNodeConfig
  , raftNodeLogCtx :: LogCtx (RaftT v m)
  , raftNodeMetrics :: Metrics.Metrics
  }

newtype RaftT v m a = RaftT
  { unRaftT :: ReaderT (RaftEnv v m) (StateT (RaftNodeState v) m) a
  } deriving newtype (Functor, Applicative, Monad, MonadReader (RaftEnv v m), MonadState (RaftNodeState v), MonadFail, Alternative, MonadPlus)

instance MonadTrans (RaftT v) where
  lift = RaftT . lift . lift

deriving newtype instance MonadIO m => MonadIO (RaftT v m)
deriving newtype instance MonadThrow m => MonadThrow (RaftT v m)
deriving newtype instance MonadCatch m => MonadCatch (RaftT v m)
deriving newtype instance MonadMask m => MonadMask (RaftT v m)

instance MonadRaftFork m => MonadRaftFork (RaftT v m) where
  type RaftThreadId (RaftT v m) = RaftThreadId m
  raftFork s m = do
    raftEnv <- ask
    raftState <- get
    lift $ raftFork s (runRaftT raftState raftEnv m)

instance Monad m => RaftLogger v (RaftT v m) where
  loggerCtx = (,) <$> asks (raftConfigNodeId . raftNodeConfig) <*> get

instance Monad m => Metrics.MonadMetrics (RaftT v m) where
  getMetrics = asks raftNodeMetrics

initializeRaftEnv
  :: MonadIO m
  => RaftEventChan v m
  -> m ()
  -> m ()
  -> RaftNodeConfig
  -> LogCtx (RaftT v m)
  -> m (RaftEnv v m)
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
  => RaftNodeState v
  -> RaftEnv v m
  -> RaftT v m a
  -> m a
runRaftT raftNodeState raftEnv =
  flip evalStateT raftNodeState . flip runReaderT raftEnv . unRaftT

------------------------------------------------------------------------------
-- Logging
------------------------------------------------------------------------------

logInfo :: MonadIO m => Text -> RaftT v m ()
logInfo msg = flip logInfoIO msg =<< asks raftNodeLogCtx

logDebug :: MonadIO m => Text -> RaftT v m ()
logDebug msg = flip logDebugIO msg =<< asks raftNodeLogCtx

logCritical :: MonadIO m => Text -> RaftT v m ()
logCritical msg = flip logCriticalIO msg =<< asks raftNodeLogCtx

logAndPanic :: MonadIO m => Text -> RaftT v m a
logAndPanic msg = flip logAndPanicIO msg =<< asks raftNodeLogCtx
