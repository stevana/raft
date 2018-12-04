{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Raft
  (
  -- * State machine type class
    RSMP(..)
  , RSM(..)

  -- * Networking type classes
  , RaftSendRPC(..)
  , RaftRecvRPC(..)
  , RaftSendClient(..)
  , RaftRecvClient(..)
  , RaftPersist(..)

  , EventChan

  , RaftEnv(..)
  , runRaftNode
  , runRaftT

  , handleEventLoop

  -- * Client data types
  , ClientRequest(..)
  , ClientReq(..)
  , ClientResponse(..)
  , ClientReadResp(..)
  , ClientWriteResp(..)
  , ClientRedirResp(..)

  -- * Configuration
  , NodeConfig(..)

  -- * Events
  , Event(..)
  , Timeout(..)
  , MessageEvent(..)

  -- * Log
  , Entry(..)
  , Entries
  , RaftWriteLog(..)
  , DeleteSuccess(..)
  , RaftDeleteLog(..)
  , RaftReadLog (..)
  , RaftLog
  , RaftLogError(..)
  , RaftLogExceptions(..)

  -- * Logging
  , LogDest(..)
  , Severity(..)

  -- * Raft node states
  , Mode(..)
  , RaftNodeState(..)
  , NodeState(..)
  , CurrentLeader(..)
  , FollowerState(..)
  , CandidateState(..)
  , LeaderState(..)
  , initRaftNodeState
  , isFollower
  , isCandidate
  , isLeader
  , setLastLogEntryData
  , getLastLogEntryData
  , getLastAppliedAndCommitIndex

  -- * Persistent state
  , PersistentState(..)
  , initPersistentState

  -- * Basic types
  , NodeId
  , NodeIds
  , ClientId(..)
  , LeaderId(..)
  , Term(..)
  , Index(..)
  , term0
  , index0

  -- * RPC
  , RPC(..)
  , RPCType(..)
  , RPCMessage(..)
  , AppendEntries(..)
  , AppendEntriesResponse(..)
  , RequestVote(..)
  , RequestVoteResponse(..)
  , AppendEntriesData(..)
  ) where

import Protolude hiding (STM, TChan, newTChan, readTChan, writeTChan, atomically)

import Control.Monad.Conc.Class
import Control.Concurrent.STM.Timer
import Control.Concurrent.Classy.STM.TChan
import Control.Concurrent.Classy.Async

import Control.Monad.Fail
import Control.Monad.Catch
import Control.Monad.Trans.Class

import qualified Data.Map as Map
import Data.Sequence (Seq(..), singleton)

import Raft.Action
import Raft.Client
import Raft.Config
import Raft.Event
import Raft.Handle
import Raft.Log
import Raft.Logging hiding (logInfo, logDebug, logCritical)
import Raft.Monad hiding (logInfo, logDebug)
import Raft.NodeState
import Raft.Persistent
import Raft.RPC
import Raft.Types


type EventChan m v = TChan (STM m) (Event v)

-- | The raft server environment composed of the concurrent variables used in
-- the effectful raft layer.
data RaftEnv v m = RaftEnv
  { eventChan :: EventChan m v
  , resetElectionTimer :: m ()
  , resetHeartbeatTimer :: m ()
  , raftNodeConfig :: NodeConfig
  , raftNodeLogDest :: LogDest
  }

newtype RaftT v m a = RaftT
  { unRaftT :: ReaderT (RaftEnv v m) (StateT RaftNodeState m) a
  } deriving (Functor, Applicative, Monad, MonadReader (RaftEnv v m), MonadState RaftNodeState, MonadFail, Alternative, MonadPlus)

instance MonadTrans (RaftT v) where
  lift = RaftT . lift . lift

deriving instance MonadIO m => MonadIO (RaftT v m)
deriving instance MonadThrow m => MonadThrow (RaftT v m)
deriving instance MonadCatch m => MonadCatch (RaftT v m)
deriving instance MonadMask m => MonadMask (RaftT v m)
deriving instance MonadConc m => MonadConc (RaftT v m)

instance Monad m => RaftLogger (RaftT v m) where
  loggerNodeId = asks (configNodeId . raftNodeConfig)
  loggerNodeState = get

runRaftT
  :: MonadConc m
  => RaftNodeState
  -> RaftEnv v m
  -> RaftT v m ()
  -> m ()
runRaftT raftNodeState raftEnv =
  flip evalStateT raftNodeState . flip runReaderT raftEnv . unRaftT

------------------------------------------------------------------------------

logDebug :: MonadIO m => Text -> RaftT v m ()
logDebug msg = flip logDebugIO msg =<< asks raftNodeLogDest

logCritical :: MonadIO m => Text -> RaftT v m ()
logCritical msg = flip logCriticalIO msg =<< asks raftNodeLogDest

------------------------------------------------------------------------------

-- | Run timers, RPC and client request handlers and start event loop.
-- It should run forever
runRaftNode
  :: ( Show v, Show sm, Show (Action sm v), Show (RaftLogError m)
     , MonadIO m, MonadConc m, MonadFail m
     , RSM sm v m
     , Show (RSMPError sm v)
     , RaftSendRPC m v
     , RaftRecvRPC m v
     , RaftSendClient m sm
     , RaftRecvClient m v
     , RaftLog m v
     , RaftLogExceptions m
     , RaftPersist m
     , Exception (RaftPersistError m)
     )
   => NodeConfig           -- ^ Node configuration
   -> LogDest              -- ^ Logs destination
   -> Int                  -- ^ Timer seed
   -> sm                   -- ^ Initial state machine state
   -> m ()
runRaftNode nodeConfig@NodeConfig{..} logDest timerSeed initRSM = do
  eventChan <- atomically newTChan

  electionTimer <- newTimerRange timerSeed configElectionTimeout
  heartbeatTimer <- newTimer configHeartbeatTimeout

  let resetElectionTimer = resetTimer electionTimer
      resetHeartbeatTimer = resetTimer heartbeatTimer
      raftEnv = RaftEnv eventChan resetElectionTimer resetHeartbeatTimer nodeConfig logDest

  runRaftT initRaftNodeState raftEnv $ do

    -- Fork all event producers to run concurrently
    lift $ fork (electionTimeoutTimer electionTimer eventChan)
    lift $ fork (heartbeatTimeoutTimer heartbeatTimer eventChan)
    fork (rpcHandler eventChan)
    fork (clientReqHandler eventChan)

    -- Start the main event handling loop
    handleEventLoop initRSM

handleEventLoop
  :: forall sm v m.
     ( Show v, Show sm, Show (Action sm v), Show (RaftLogError m)
     , MonadIO m, MonadConc m, MonadFail m
     , RSM sm v m
     , Show (RSMPError sm v)
     , RaftPersist m
     , RaftSendRPC m v
     , RaftSendClient m sm
     , RaftLog m v
     , RaftLogExceptions m
     , RaftPersist m
     , Exception (RaftPersistError m)
     )
  => sm
  -> RaftT v m ()
handleEventLoop initRSM = do
    ePersistentState <- lift readPersistentState
    case ePersistentState of
      Left err -> throw err
      Right pstate -> handleEventLoop' initRSM pstate
  where
    handleEventLoop' :: sm -> PersistentState -> RaftT v m ()
    handleEventLoop' stateMachine persistentState = do
      event <- atomically . readTChan =<< asks eventChan
      loadLogEntryTermAtAePrevLogIndex event
      raftNodeState <- get
      logDebug $ "[Event]: " <> show event
      logDebug $ "[NodeState]: " <> show raftNodeState
      logDebug $ "[State Machine]: " <> show stateMachine
      logDebug $ "[Persistent State]: " <> show persistentState
      -- Perform core state machine transition, handling the current event
      nodeConfig <- asks raftNodeConfig
      let transitionEnv = TransitionEnv nodeConfig stateMachine raftNodeState
          (resRaftNodeState, resPersistentState, actions, logMsgs) =
            Raft.Handle.handleEvent raftNodeState transitionEnv persistentState event
      -- Write persistent state to disk
      eRes <- lift $ writePersistentState resPersistentState
      case eRes of
        Left err -> throw err
        Right _ -> pure ()
      -- Update raft node state with the resulting node state
      put resRaftNodeState
      -- Handle logs producek by core state machine
      handleLogs logMsgs
      -- Handle actions produced by core state machine
      handleActions nodeConfig actions
      -- Apply new log entries to the state machine
      resRSM <- applyLogEntries stateMachine
      handleEventLoop' resRSM resPersistentState

    -- In the case that a node is a follower receiving an AppendEntriesRPC
    -- Event, read the log at the aePrevLogIndex
    loadLogEntryTermAtAePrevLogIndex :: Event v -> RaftT v m ()
    loadLogEntryTermAtAePrevLogIndex event =
      case event of
        MessageEvent (RPCMessageEvent (RPCMessage _ (AppendEntriesRPC ae))) -> do
          RaftNodeState rns <- get
          case rns of
            NodeFollowerState fs -> do
              eEntry <- lift $ readLogEntry (aePrevLogIndex ae)
              case eEntry of
                Left err -> throw err
                Right (mEntry :: Maybe (Entry v)) -> put $
                  RaftNodeState $ NodeFollowerState fs
                    { fsTermAtAEPrevIndex = entryTerm <$> mEntry }
            _ -> pure ()
        _ -> pure ()

handleActions
  :: ( Show v, Show sm, Show (Action sm v), Show (RaftLogError m)
     , MonadIO m, MonadConc m
     , RSM sm v m
     , RaftSendRPC m v
     , RaftSendClient m sm
     , RaftLog m v
     , RaftLogExceptions m
     )
  => NodeConfig
  -> [Action sm v]
  -> RaftT v m ()
handleActions = mapM_ . handleAction

handleAction
  :: forall sm v m.
     ( Show v, Show sm, Show (Action sm v), Show (RaftLogError m)
     , MonadIO m, MonadConc m
     , RSM sm v m
     , RaftSendRPC m v
     , RaftSendClient m sm
     , RaftLog m v
     , RaftLogExceptions m
     )
  => NodeConfig
  -> Action sm v
  -> RaftT v m ()
handleAction nodeConfig action = do
  logDebug $ "[Action]: " <> show action
  case action of
    SendRPC nid sendRpcAction -> do
      rpcMsg <- mkRPCfromSendRPCAction sendRpcAction
      lift (sendRPC nid rpcMsg)
    SendRPCs rpcMap ->
      forConcurrently_ (Map.toList rpcMap) $ \(nid, sendRpcAction) -> do
        rpcMsg <- mkRPCfromSendRPCAction sendRpcAction
        lift (sendRPC nid rpcMsg)
    BroadcastRPC nids sendRpcAction -> do
      rpcMsg <- mkRPCfromSendRPCAction sendRpcAction
      mapConcurrently_ (lift . flip sendRPC rpcMsg) nids
    RespondToClient cid cr -> lift $ sendClient cid cr
    ResetTimeoutTimer tout ->
      case tout of
        ElectionTimeout -> lift . resetElectionTimer =<< ask
        HeartbeatTimeout -> lift . resetHeartbeatTimer =<< ask
    AppendLogEntries entries -> do
      eRes <- lift (updateLog entries)
      case eRes of
        Left err -> panic (show err)
        Right _ -> do
          -- Update the last log entry data
          modify $ \(RaftNodeState ns) ->
            RaftNodeState (setLastLogEntryData ns entries)

  where
    mkRPCfromSendRPCAction :: SendRPCAction v -> RaftT v m (RPCMessage v)
    mkRPCfromSendRPCAction sendRPCAction = do
      RaftNodeState ns <- get
      RPCMessage (configNodeId nodeConfig) <$>
        case sendRPCAction of
          SendAppendEntriesRPC aeData -> do
            (entries, prevLogIndex, prevLogTerm, aeReadReq) <-
              case aedEntriesSpec aeData of
                FromIndex idx -> do
                  eLogEntries <- lift (readLogEntriesFrom (decrIndexWithDefault0 idx))
                  case eLogEntries of
                    Left err -> throw err
                    Right log ->
                      case log of
                        pe :<| entries@(e :<| _)
                          | idx == 1 -> pure (log, index0, term0, Nothing)
                          | otherwise -> pure (entries, entryIndex pe, entryTerm pe, Nothing)
                        _ -> pure (log, index0, term0, Nothing)
                FromClientWriteReq e -> prevEntryData e
                FromNewLeader e -> prevEntryData e
                NoEntries spec -> do
                  let readReq =
                        case spec of
                          FromClientReadReq n -> Just n
                          _ -> Nothing
                      (lastLogIndex, lastLogTerm) = getLastLogEntryData ns
                  pure (Empty, lastLogIndex, lastLogTerm, readReq)
            let leaderId = LeaderId (configNodeId nodeConfig)
            pure . toRPC $
              AppendEntries
                { aeTerm = aedTerm aeData
                , aeLeaderId = leaderId
                , aePrevLogIndex = prevLogIndex
                , aePrevLogTerm = prevLogTerm
                , aeEntries = entries
                , aeLeaderCommit = aedLeaderCommit aeData
                , aeReadRequest = aeReadReq
                }
          SendAppendEntriesResponseRPC aer -> pure (toRPC aer)
          SendRequestVoteRPC rv -> pure (toRPC rv)
          SendRequestVoteResponseRPC rvr -> pure (toRPC rvr)

    prevEntryData e = do
      (x,y,z) <- prevEntryData' e
      pure (x,y,z,Nothing)

    prevEntryData' e
      | entryIndex e == Index 1 = pure (singleton e, index0, term0)
      | otherwise = do
          let prevLogEntryIdx = decrIndexWithDefault0 (entryIndex e)
          eLogEntry <- lift $ readLogEntry prevLogEntryIdx
          case eLogEntry of
            Left err -> throw err
            Right Nothing -> pure (singleton e, index0, term0)
            Right (Just (prevEntry :: Entry v)) ->
              pure (singleton e, entryIndex prevEntry, entryTerm prevEntry)

-- If commitIndex > lastApplied: increment lastApplied, apply
-- log[lastApplied] to state machine (Section 5.3) until the state machine
-- is up to date with all the committed log entries
applyLogEntries
  :: forall sm m v.
     ( Show sm
     , MonadConc m
     , RaftReadLog m v
     , Exception (RaftReadLogError m)
     , RSM sm v m
     , Show (RSMPError sm v)
     )
  => sm
  -> RaftT v m sm
applyLogEntries stateMachine = do
    raftNodeState@(RaftNodeState nodeState) <- get
    if commitIndex nodeState > lastApplied nodeState
      then do
        let resNodeState = incrLastApplied nodeState
        put $ RaftNodeState resNodeState
        let newLastAppliedIndex = lastApplied resNodeState
        eLogEntry <- lift $ readLogEntry newLastAppliedIndex
        case eLogEntry of
          Left err -> throw err
          Right Nothing -> panic $ "No log entry at 'newLastAppliedIndex := " <> show newLastAppliedIndex <> "'"
          Right (Just logEntry) -> do
            -- The command should be verified by the leader, thus all node
            -- attempting to apply the committed log entry should not fail when
            -- doing so; failure here means something has gone very wrong.
            eRes <- lift (applyEntryRSM stateMachine logEntry)
            case eRes of
              Left err -> panic $ "Failed to apply committed log entry: " <> show err
              Right nsm -> applyLogEntries nsm
      else pure stateMachine
  where
    incrLastApplied :: NodeState ns -> NodeState ns
    incrLastApplied nodeState =
      case nodeState of
        NodeFollowerState fs ->
          let lastApplied' = incrIndex (fsLastApplied fs)
           in NodeFollowerState $ fs { fsLastApplied = lastApplied' }
        NodeCandidateState cs ->
          let lastApplied' = incrIndex (csLastApplied cs)
           in  NodeCandidateState $ cs { csLastApplied = lastApplied' }
        NodeLeaderState ls ->
          let lastApplied' = incrIndex (lsLastApplied ls)
           in NodeLeaderState $ ls { lsLastApplied = lastApplied' }

    lastApplied :: NodeState ns -> Index
    lastApplied = fst . getLastAppliedAndCommitIndex

    commitIndex :: NodeState ns -> Index
    commitIndex = snd . getLastAppliedAndCommitIndex

handleLogs
  :: (MonadIO m, MonadConc m)
  => [LogMsg]
  -> RaftT v m ()
handleLogs logs = do
  logDest <- asks raftNodeLogDest
  mapM_ (logToDest logDest) logs

------------------------------------------------------------------------------
-- Event Producers
------------------------------------------------------------------------------

-- | Producer for rpc message events
rpcHandler
  :: (MonadIO m, MonadConc m, Show v, RaftRecvRPC m v)
  => TChan (STM m) (Event v)
  -> RaftT v m ()
rpcHandler eventChan =
  forever $ do
    eRpcMsg <- lift $ Control.Monad.Catch.try receiveRPC
    case eRpcMsg of
      Left (err :: SomeException) -> logCritical (show err)
      Right (Left err) -> logCritical (show err)
      Right (Right rpcMsg) -> do
        let rpcMsgEvent = MessageEvent (RPCMessageEvent rpcMsg)
        atomically $ writeTChan eventChan rpcMsgEvent

-- | Producer for rpc message events
clientReqHandler
  :: (MonadIO m, MonadConc m, RaftRecvClient m v)
  => TChan (STM m) (Event v)
  -> RaftT v m ()
clientReqHandler eventChan =
  forever $ do
    eClientReq <- lift $ Control.Monad.Catch.try receiveClient
    case eClientReq of
      Left (err :: SomeException) -> logCritical (show err)
      Right (Left err) -> logCritical (show err)
      Right (Right clientReq) -> do
        let clientReqEvent = MessageEvent (ClientRequestEvent clientReq)
        atomically $ writeTChan eventChan clientReqEvent

-- | Producer for the election timeout event
electionTimeoutTimer :: MonadConc m => Timer m -> TChan (STM m) (Event v) -> m ()
electionTimeoutTimer timer eventChan =
  forever $ do
    startTimer timer >> waitTimer timer
    atomically $ writeTChan eventChan (TimeoutEvent ElectionTimeout)

-- | Producer for the heartbeat timeout event
heartbeatTimeoutTimer :: MonadConc m => Timer m -> TChan (STM m) (Event v) -> m ()
heartbeatTimeoutTimer timer eventChan =
  forever $ do
    startTimer timer >> waitTimer timer
    atomically $ writeTChan eventChan (TimeoutEvent HeartbeatTimeout)
