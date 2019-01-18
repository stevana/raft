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
  , LogCtx(..)
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
  , setLastLogEntry
  , getLastLogEntry
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

import Protolude hiding (STM, TChan, newTChan, readBoundedChan, writeBoundedChan, atomically)

import Control.Monad.Conc.Class
import Control.Concurrent.STM.Timer
import Control.Concurrent.Classy.STM.TChan
import Control.Concurrent.Classy.Async

import Control.Monad.Fail
import Control.Monad.Catch
import Control.Monad.Trans.Class

import qualified Data.Map as Map
import Data.Serialize (Serialize)
import Data.Sequence (Seq(..), singleton)
import Data.Time.Clock.System (getSystemTime)

import Raft.Action
import Raft.Client
import Raft.Config
import Raft.Event
import Raft.Handle
import Raft.Log
import Raft.Logging hiding (logInfo, logDebug, logCritical, logAndPanic)
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
  , raftNodeLogCtx :: LogCtx
  }

newtype RaftT v m a = RaftT
  { unRaftT :: ReaderT (RaftEnv v m) (StateT (RaftNodeState v) m) a
  } deriving (Functor, Applicative, Monad, MonadReader (RaftEnv v m), MonadState (RaftNodeState v), MonadFail, Alternative, MonadPlus)

instance MonadTrans (RaftT v) where
  lift = RaftT . lift . lift

deriving instance MonadIO m => MonadIO (RaftT v m)
deriving instance MonadThrow m => MonadThrow (RaftT v m)
deriving instance MonadCatch m => MonadCatch (RaftT v m)
deriving instance MonadMask m => MonadMask (RaftT v m)
deriving instance MonadConc m => MonadConc (RaftT v m)

instance Monad m => RaftLogger v (RaftT v m) where
  loggerCtx = (,) <$> asks (configNodeId . raftNodeConfig) <*> get

runRaftT
  :: MonadConc m
  => RaftNodeState v
  -> RaftEnv v m
  -> RaftT v m ()
  -> m ()
runRaftT raftNodeState raftEnv =
  flip evalStateT raftNodeState . flip runReaderT raftEnv . unRaftT

------------------------------------------------------------------------------

logDebug :: MonadIO m => Text -> RaftT v m ()
logDebug msg = flip logDebugIO msg =<< asks raftNodeLogCtx

logCritical :: MonadIO m => Text -> RaftT v m ()
logCritical msg = flip logCriticalIO msg =<< asks raftNodeLogCtx

logAndPanic :: MonadIO m => Text -> RaftT v m a
logAndPanic msg = flip logAndPanicIO msg =<< asks raftNodeLogCtx

------------------------------------------------------------------------------

-- | Run timers, RPC and client request handlers and start event loop.
-- It should run forever
runRaftNode
  :: forall m sm v.
     ( Show v, Show sm, Serialize v, Show (Action sm v), Show (RaftLogError m), Typeable m
     , MonadIO m, MonadConc m, MonadFail m
     , RSM sm v m
     , Show (RSMPError sm v)
     , RaftSendRPC m v
     , RaftRecvRPC m v
     , RaftSendClient m sm v
     , RaftRecvClient m v
     , RaftLog m v
     , RaftLogExceptions m
     , RaftPersist m
     , Exception (RaftPersistError m)
     )
   => NodeConfig           -- ^ Node configuration
   -> LogCtx               -- ^ Logs destination
   -> Int                  -- ^ Timer seed
   -> sm                   -- ^ Initial state machine state
   -> m ()
runRaftNode nodeConfig@NodeConfig{..} logCtx timerSeed initRSM = do
  -- Initialize the persistent state and logs storage if specified
  initializeStorage

  eventChan <- atomically newTChan

  electionTimer <-
    newTimerRange (writeTimeoutEvent eventChan ElectionTimeout) timerSeed configElectionTimeout

  heartbeatTimer <-
    newTimer (writeTimeoutEvent eventChan HeartbeatTimeout) configHeartbeatTimeout

  let resetElectionTimer = void $ resetTimer electionTimer
      resetHeartbeatTimer = void $ resetTimer heartbeatTimer
      raftEnv = RaftEnv eventChan resetElectionTimer resetHeartbeatTimer nodeConfig logCtx

  runRaftT initRaftNodeState raftEnv $ do

    -- Fork all event producers to run concurrently
    lift $ fork (electionTimeoutTimer electionTimer)
    lift $ fork (heartbeatTimeoutTimer heartbeatTimer)
    fork (rpcHandler eventChan)
    fork (clientReqHandler eventChan)

    -- Start the main event handling loop
    handleEventLoop initRSM

  where
    initializeStorage =
      case configStorageState of
        New -> do
          ipsRes <- initializePersistentState
          case ipsRes of
            Left err -> throw err
            Right _ -> do
              ilRes <- initializeLog (Proxy :: Proxy v)
              case ilRes of
                Left err -> throw err
                Right _ -> pure ()
        Existing -> pure ()

handleEventLoop
  :: forall sm v m.
     ( Show v, Serialize v, Show sm, Show (Action sm v), Show (RaftLogError m), Typeable m
     , MonadIO m, MonadConc m, MonadFail m
     , RSM sm v m
     , Show (RSMPError sm v)
     , RaftPersist m
     , RaftSendRPC m v
     , RaftSendClient m sm v
     , RaftLog m v
     , RaftLogExceptions m
     , RaftPersist m
     , Exception (RaftPersistError m)
     )
  => sm
  -> RaftT v m ()
handleEventLoop initRSM = do
    setInitLastLogEntry
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

      -- Write persistent state to disk.
      --
      -- Checking equality of Term + NodeId (what PersistentState is comprised of)
      -- is very cheap, but writing to disk is not necessarily cheap.
      when (resPersistentState /= persistentState) $ do
        eRes <- lift $ writePersistentState resPersistentState
        case eRes of
          Left err -> throw err
          Right _ -> pure ()

      -- Update raft node state with the resulting node state
      put resRaftNodeState
      -- Handle logs produced by core state machine
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

    -- Load the last log entry from a existing log
    setInitLastLogEntry :: RaftT v m ()
    setInitLastLogEntry = do
      RaftNodeState rns <- get
      eLogEntry <- lift readLastLogEntry
      case eLogEntry of
        Left err -> throw err
        Right Nothing -> pure ()
        Right (Just e) ->
          put (RaftNodeState (setLastLogEntry rns (singleton e)))

handleActions
  :: ( Show v, Show sm, Show (Action sm v), Show (RaftLogError m), Typeable m
     , MonadIO m, MonadConc m
     , RSM sm v m
     , RaftSendRPC m v
     , RaftSendClient m sm v
     , RaftLog m v
     , RaftLogExceptions m
     )
  => NodeConfig
  -> [Action sm v]
  -> RaftT v m ()
handleActions = mapM_ . handleAction

handleAction
  :: forall sm v m.
     ( Show v, Show sm, Show (Action sm v), Show (RaftLogError m), Typeable m
     , MonadIO m, MonadConc m
     , RSM sm v m
     , RaftSendRPC m v
     , RaftSendClient m sm v
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
    RespondToClient cid cr -> do
      clientResp <- mkClientResp cr
      void . fork . lift $ sendClient cid clientResp
    ResetTimeoutTimer tout -> do
      case tout of
        ElectionTimeout -> lift . resetElectionTimer =<< ask
        HeartbeatTimeout -> lift . resetHeartbeatTimer =<< ask
    AppendLogEntries entries -> do
      eRes <- lift (updateLog entries)
      case eRes of
        Left err -> logAndPanic (show err)
        Right _ -> do
          -- Update the last log entry data
          modify $ \(RaftNodeState ns) ->
            RaftNodeState (setLastLogEntry ns entries)
    UpdateClientReqCacheFrom idx -> do
      RaftNodeState ns <- get
      case ns of
        NodeLeaderState ls@LeaderState{..} -> do
          eentries <- lift (readLogEntriesFrom idx)
          case eentries of
            Left err -> throw err
            Right (entries :: Entries v) ->  do
              let committedClientReqs = clientReqData entries
              when (Map.size committedClientReqs > 0) $ do
                mapM_ respondClientWrite (Map.toList committedClientReqs)
                let creqMap = Map.map (second Just) committedClientReqs
                put $ RaftNodeState $ NodeLeaderState
                  ls { lsClientReqCache = creqMap `Map.union` lsClientReqCache }
        _ -> logAndPanic "Only the leader should update the client request cache..."
  where
    respondClientWrite :: (ClientId, (SerialNum, Index)) -> RaftT v m ()
    respondClientWrite (cid, (sn,idx)) =
      handleAction nodeConfig $
        RespondToClient cid (ClientWriteRespSpec idx sn :: ClientRespSpec sm)

    mkClientResp :: ClientRespSpec sm -> RaftT v m (ClientResponse sm v)
    mkClientResp crs =
      case crs of
        ClientReadRespSpec crrs ->
          ClientReadResponse <$>
            case crrs of
              ClientReadRespSpecEntries res -> do
                eRes <- lift (readEntries res)
                case eRes of
                  Left err -> throw err
                  Right res ->
                    case res of
                      OneEntry e -> pure (ClientReadRespEntry e)
                      ManyEntries es -> pure (ClientReadRespEntries es)
              ClientReadRespSpecStateMachine sm ->
                pure (ClientReadRespStateMachine sm)
        ClientWriteRespSpec idx sn ->
          pure (ClientWriteResponse (ClientWriteResp idx sn))
        ClientRedirRespSpec cl ->
          pure (ClientRedirectResponse (ClientRedirResp cl))

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
                      (lastLogIndex, lastLogTerm) = lastLogEntryIndexAndTerm (getLastLogEntry ns)
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
     , MonadIO m
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
          Right Nothing -> logAndPanic $ "No log entry at 'newLastAppliedIndex := " <> show newLastAppliedIndex <> "'"
          Right (Just logEntry) -> do
            -- The command should be verified by the leader, thus all node
            -- attempting to apply the committed log entry should not fail when
            -- doing so; failure here means something has gone very wrong.
            eRes <- lift (applyEntryRSM stateMachine logEntry)
            case eRes of
              Left err -> logAndPanic $ "Failed to apply committed log entry: " <> show err
              Right nsm -> applyLogEntries nsm
      else pure stateMachine
  where
    incrLastApplied :: NodeState ns v -> NodeState ns v
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

    lastApplied :: NodeState ns v -> Index
    lastApplied = fst . getLastAppliedAndCommitIndex

    commitIndex :: NodeState ns v -> Index
    commitIndex = snd . getLastAppliedAndCommitIndex

handleLogs
  :: (MonadIO m, MonadConc m)
  => [LogMsg]
  -> RaftT v m ()
handleLogs logs = do
  logCtx <- asks raftNodeLogCtx
  mapM_ (logToDest logCtx) logs

------------------------------------------------------------------------------
-- Event Producers
------------------------------------------------------------------------------

-- | Producer for rpc message events
rpcHandler
  :: (MonadIO m, MonadConc m, Show v, RaftRecvRPC m v)
  => EventChan m v
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
  => EventChan m v
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
electionTimeoutTimer :: (MonadIO m, MonadConc m) => Timer m -> m ()
electionTimeoutTimer timer =
  forever $ do
    success <- startTimer timer
    when (not success) $
      panic "[Failed invariant]: Election timeout timer failed to start."
    waitTimer timer

-- | Producer for the heartbeat timeout event
heartbeatTimeoutTimer :: (MonadIO m, MonadConc m) => Timer m -> m ()
heartbeatTimeoutTimer timer =
  forever $ do
    success <- startTimer timer
    when (not success) $
      panic "[Failed invariant]: Heartbeat timeout timer failed to start."
    waitTimer timer

writeTimeoutEvent :: (MonadIO m , MonadConc m) => EventChan m v -> Timeout -> m ()
writeTimeoutEvent eventChan timeout = do
  now <- liftIO getSystemTime
  atomically $ writeTChan eventChan (TimeoutEvent now timeout)
