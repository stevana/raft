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
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE TypeApplications #-}

module Raft
  (
  -- * State machine type class
    RaftStateMachinePure(..)
  , RaftStateMachine(..)

  -- * Networking type classes
  , RaftSendRPC(..)
  , RaftRecvRPC(..)
  , RaftSendClient(..)
  , RaftRecvClient(..)
  , RaftPersist(..)

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
  , RaftNodeConfig(..)

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

import Protolude hiding (STM, TChan, newRaftChan, readBoundedChan, writeBoundedChan, atomically)

import Control.Concurrent.STM.Timer

import Control.Monad.Fail
import Control.Monad.Catch

import qualified Data.Map as Map
import Data.Serialize (Serialize)
import Data.Sequence (Seq(..), singleton)
import Data.Time.Clock.System (getSystemTime)

import Raft.Action
import Raft.Client
import Raft.Config
import Raft.Event
import Raft.Handle
import Raft.Monad
import Raft.Log
import Raft.Logging hiding (logInfo, logDebug, logCritical, logAndPanic)
import Raft.Transition hiding (logInfo, logDebug)
import Raft.NodeState
import Raft.Persistent
import Raft.RPC
import Raft.StateMachine
import Raft.Types
import qualified Raft.Metrics as Metrics

import qualified System.Remote.Monitoring as EKG

-- | Run timers, RPC and client request handlers and start event loop.
-- It should run forever
runRaftNode
  :: forall m sm v.
     ( Typeable m, Show v, Show sm, Serialize v, Show (Action sm v), Show (RaftLogError m), Show (RaftStateMachinePureError sm v)
     , MonadIO m, MonadCatch m, MonadFail m, MonadMask m, MonadRaft v m
     , RaftStateMachine m sm v
     , RaftSendRPC m v
     , RaftRecvRPC m v
     , RaftSendClient m sm v
     , RaftRecvClient m v
     , RaftLog m v
     , RaftLogExceptions m
     , RaftPersist m
     , Exception (RaftPersistError m)
     )
   => RaftNodeConfig         -- ^ Node configuration
   -> OptionalRaftNodeConfig -- ^ Config values that can be provided optionally
   -> LogCtx (RaftT sm v m)     -- ^ The means with which to log messages
   -> sm                     -- ^ Initial state machine state
   -> m ()
runRaftNode nodeConfig@RaftNodeConfig{..} optConfig logCtx initStateMachine = do

  -- Resolve the optional config values
  metricsPort <- liftIO (resolveMetricsPort (raftConfigMetricsPort optConfig))
  timerSeed <- liftIO (resolveTimerSeed (raftConfigTimerSeed optConfig))


  -- Initialize the persistent state and logs storage if specified
  initializeStorage raftConfigStorageState

  -- Initialize the main event channel
  eventChan <- newRaftChan @v

  -- Create timers and reset timer actions
  electionTimer <- liftIO $ newTimerRange timerSeed raftConfigElectionTimeout
  heartbeatTimer <- liftIO $ newTimer raftConfigHeartbeatTimeout
  let resetElectionTimer = liftIO $ void $ resetTimer electionTimer
      resetHeartbeatTimer = liftIO $ void $ resetTimer heartbeatTimer

  raftEnv <- initializeRaftEnv eventChan resetElectionTimer resetHeartbeatTimer nodeConfig logCtx
  runRaftT initRaftNodeState raftEnv $ do

    -- Fork the monitoring server for metrics
    case metricsPort of
      Nothing -> pure ()
      Just port -> do
        logInfo ("Forking metrics server on port " <> show port <> "...")
        metricsStore <- Metrics.getMetricsStore
        void $ liftIO (EKG.forkServerWith metricsStore "localhost" (fromIntegral port))

    logInfo ("Initialized election timer with seed " <> show timerSeed <> "...")

    -- These event producers need access to logging, thus they live in RaftT
    --
    -- Note: Changing the roles of these event producers (the strings passed as
    -- arguments to 'raftFork' should incur a minor (or major?) version bump,
    -- because some implementations of 'MonadRaftFork' rely on these strings to
    -- reliably send messages to the event producers (e.g. cloud-haskell
    -- processes).
    raftFork (CustomThreadRole "Election Timeout Timer") $
      electionTimeoutTimer electionTimer
    raftFork (CustomThreadRole "Heartbeat Timeout Timer") $
      heartbeatTimeoutTimer heartbeatTimer
    raftFork RPCHandler rpcHandler
    raftFork ClientRequestHandler clientReqHandler

    -- Start the main event handling loop
    handleEventLoop initStateMachine

  where
    initializeStorage storageState =
      case storageState of
        New -> do
          ipsRes <- initializePersistentState
          case ipsRes of
            Left err -> throwM err
            Right _ -> do
              ilRes <- initializeLog (Proxy :: Proxy v)
              case ilRes of
                Left err -> throwM err
                Right _ -> pure ()
        Existing -> pure ()

handleEventLoop
  :: forall sm v m.
     ( Show v, Serialize v, Show sm, Show (Action sm v), Show (RaftLogError m), Typeable m
     , MonadIO m, MonadRaft v m, MonadFail m, MonadThrow m, MonadMask m
     , RaftStateMachine m sm v
     , Show (RaftStateMachinePureError sm v)
     , RaftPersist m
     , RaftSendRPC m v
     , RaftSendClient m sm v
     , RaftLog m v
     , RaftLogExceptions m
     , RaftReadLog m v
     , Show v
     , RaftPersist m
     , Exception (RaftPersistError m)
     )
  => sm
  -> RaftT sm v m ()
handleEventLoop initStateMachine = do
    ePersistentState <- lift readPersistentState
    case ePersistentState of
      Left err -> throwM err
      Right pstate -> handleEventLoop' initStateMachine pstate
  where

    -- Some events must be validated before being processed by the system:
    --
    --   - If the node is a leader, and the event is a client write request, the
    --   command issued by the client must satisfy a predicate given by the
    --   programmer in the 'RaftStateMachine` and 'RaftStateMachinePure' typeclasses
    --
    -- This function returns 'Nothing' if the event was not valid, and returns
    -- the result of the continuation function wrapped in 'Just', otherwise.
    withValidatedEvent :: (Event v -> RaftT sm v m a) -> RaftT sm v m (Maybe a)
    withValidatedEvent f = do
      event <- readRaftEventChan
      RaftNodeState raftNodeState <- get
      case raftNodeState of
        NodeLeaderState ls -> do
          case event of
            MessageEvent (ClientRequestEvent (ClientRequest cid creq)) ->
              case creq of
                ClientWriteReq (ClientCmdReq serial cmd) -> do
                  -- apply log to the tentative state machine comprised of the
                  -- current state machine plus all commands that are waiting
                  -- to be replicated to all followers
                  eRes <- lift (applyLogCmd MonadicValidation (lsStateMachine ls) cmd)
                  case eRes of
                    Left err -> do
                      -- Increments the number of invalid commands seen during
                      -- the lifetime of this node.
                      Metrics.incrInvalidCmdCounter
                      let clientWriteRespSpec = ClientWriteRespSpec (ClientWriteRespSpecFail serial err)
                          clientFailRespAction = RespondToClient cid clientWriteRespSpec
                      handleAction clientFailRespAction
                      pure Nothing
                    -- Update the leader cumulative state machine for future
                    -- validation of new client write request commands
                    Right lsStateMachine' -> do
                      let lsNewStateMachine = ls { lsStateMachine = lsStateMachine' }
                      put (RaftNodeState (NodeLeaderState lsNewStateMachine))
                      Just <$> f event
                _ -> Just <$> f event
            _ -> Just <$> f event
        _ -> Just <$> f event

    handleEventLoop' :: sm -> PersistentState -> RaftT sm v m ()
    handleEventLoop' stateMachine persistentState = do

      setInitLastLogEntry

      mRes <-
        withValidatedEvent $ \event -> do
          loadLogEntryTermAtAePrevLogIndex event
          raftNodeState@(RaftNodeState nodeState) <- get

          -- Record the current node state as a metric
          Metrics.setNodeStateLabel (nodeMode raftNodeState)
          Metrics.setCommitIndexGauge (getCommitIndex nodeState)
          numEventsInChan <- Metrics.getRaftNodeNumEventsInChan

          logDebug $ "[# Events in Chan]: " <> show numEventsInChan
          logDebug $ "[Event]: " <> show event
          logDebug $ "[NodeState]: " <> show raftNodeState
          logDebug $ "[State Machine]: " <> show stateMachine
          logDebug $ "[Persistent State]: " <> show persistentState
          -- Perform core state machine transition, handling the current event
          nodeConfig <- asks raftNodeConfig
          raftNodeMetrics <- Metrics.getRaftNodeMetrics
          let transitionEnv = TransitionEnv nodeConfig stateMachine raftNodeState raftNodeMetrics
          pure (Raft.Handle.handleEvent raftNodeState transitionEnv persistentState event)

      case mRes of
        Nothing -> handleEventLoop' stateMachine persistentState
        Just (resRaftNodeState, resPersistentState, actions, logMsgs) -> do
          logDebug "Writing PersistentState to disk..."
          -- Write persistent state to disk.
          --
          -- Checking equality of Term + NodeId (what PersistentState is comprised
          -- of) is very cheap, but writing to disk is not necessarily cheap.
          when (resPersistentState /= persistentState) $ do
            eRes <- lift $ writePersistentState resPersistentState
            case eRes of
              Left err -> throwM err
              Right _ -> pure ()

          -- Update raft node state with the resulting node state
          put resRaftNodeState
          -- Handle logs produced by core state machine
          handleLogs logMsgs
          -- Handle actions produced by core state machine
          handleActions actions
          -- Apply new log entries to the state machine
          resRaftStateMachine <- applyLogEntries stateMachine
          handleEventLoop' resRaftStateMachine resPersistentState

    -- In the case that a node is a follower receiving an AppendEntriesRPC
    -- Event, read the log at the aePrevLogIndex
    loadLogEntryTermAtAePrevLogIndex :: Event v -> RaftT sm v m ()
    loadLogEntryTermAtAePrevLogIndex event =
      case event of
        MessageEvent (RPCMessageEvent (RPCMessage _ (AppendEntriesRPC ae))) -> do
          RaftNodeState rns <- get
          case rns of
            NodeFollowerState fs -> do
              eEntry <- lift $ readLogEntry (aePrevLogIndex ae)
              case eEntry of
                Left err -> throwM err
                Right (mEntry :: Maybe (Entry v)) -> put $
                  RaftNodeState $ NodeFollowerState fs
                    { fsTermAtAEPrevIndex = entryTerm <$> mEntry }
            _ -> pure ()
        _ -> pure ()

    -- Load the last log entry from a existing log
    setInitLastLogEntry :: RaftT sm v m ()
    setInitLastLogEntry = do
      RaftNodeState rns <- get
      eLogEntry <- lift readLastLogEntry
      case eLogEntry of
        Left err -> throwM err
        Right Nothing -> pure ()
        Right (Just e) ->
          put (RaftNodeState (setLastLogEntry rns (singleton e)))

-- | Handles all actions produced by the main 'handleEventLoop'' call.
handleActions
  :: ( Show v, Serialize v, Show sm, Show (Action sm v), Show (RaftLogError m)
     , Typeable m, MonadIO m, MonadRaft v m, MonadThrow m, MonadMask m
     , RaftStateMachine m sm v
     , RaftSendRPC m v
     , RaftSendClient m sm v
     , RaftLog m v
     , RaftLogExceptions m
     )
  => [Action sm v]
  -> RaftT sm v m ()
handleActions actions = do
  mapM_ handleAction actions

handleAction
  :: forall sm v m.
     ( Show v, Serialize v, Show sm, Show (Action sm v)
     , Show (RaftLogError m), Typeable m, MonadIO m, MonadRaft v m, MonadThrow m
     , RaftStateMachine m sm v
     , RaftSendRPC m v
     , RaftSendClient m sm v
     , RaftLog m v
     , RaftLogExceptions m
     )
  => Action sm v
  -> RaftT sm v m ()
handleAction action = do
  logDebug $ "Handling [Action]: " <> show action
  case action of
    SendRPC nid sendRpcAction -> do
      rpcMsg <- mkRPCfromSendRPCAction sendRpcAction
      sendRPCThread nid rpcMsg
    SendRPCs rpcMap ->
      forM_ (Map.toList rpcMap) $ \(nid, sendRpcAction) -> do
        rpcMsg <- mkRPCfromSendRPCAction sendRpcAction
        sendRPCThread nid rpcMsg
    BroadcastRPC nids sendRpcAction -> do
      rpcMsg <- mkRPCfromSendRPCAction sendRpcAction
      let sendRPC' = lift . flip sendRPC rpcMsg
      forM_ nids $ \nid ->
        raftFork (CustomThreadRole "RPC Broadcast") (sendRPC' nid)
    RespondToClient cid cr -> respondToClient cid cr
    ResetTimeoutTimer tout -> do
      case tout of
        ElectionTimeout -> lift . resetElectionTimer =<< ask
        HeartbeatTimeout -> lift . resetHeartbeatTimer =<< ask
    AppendLogEntries entries -> do
      eRes <- lift (updateLog entries)
      case eRes of
        Left err -> logAndPanic (show err)
        Right mLastLogEntry -> do
          -- Update the last log entry index metrics
          case mLastLogEntry of
            Nothing -> pure ()
            Just lastLogEntry -> do
              Metrics.setLastLogEntryIndexGauge lastLogEntry
              Metrics.setLastLogEntryHashLabel lastLogEntry
          -- Update the last log entry data
          modify $ \(RaftNodeState ns) ->
            RaftNodeState (setLastLogEntry ns entries)
    UpdateClientReqCacheFrom idx -> do
      RaftNodeState ns <- get
      case ns of
        NodeLeaderState ls@LeaderState{..} -> do
          let idxInterval = IndexInterval (Just idx) (Just lsCommitIndex)
          eentries <- lift (readEntriesByIndices idxInterval)
          case eentries of
            Left err -> throwM err
            Right (entries :: Entries v) ->  do
              let committedClientReqs = clientReqData entries
              when (Map.size committedClientReqs > 0) $ do
                mapM_ respondToClientWrite (Map.toList committedClientReqs)
                let creqMap = Map.map (second Just) committedClientReqs
                put $ RaftNodeState $ NodeLeaderState
                  ls { lsClientReqCache = creqMap `Map.union` lsClientReqCache }
        _ -> logAndPanic "Only the leader should update the client request cache..."
  where
    sendRPCThread :: NodeId -> RPCMessage v -> RaftT sm v m ()
    sendRPCThread nid rpcMsg =
      void (raftFork (CustomThreadRole "Send RPC") (lift (sendRPC nid rpcMsg)))
    respondToClientWrite :: (ClientId, (SerialNum, Index)) -> RaftT sm v m ()
    respondToClientWrite (cid, (sn,idx)) = do
      let clientWriteRespSpec =
            ClientWriteRespSpec @sm (ClientWriteRespSpecSuccess idx sn)
      respondToClient cid clientWriteRespSpec

    respondToClient :: ClientId -> ClientRespSpec sm v -> RaftT sm v m ()
    respondToClient cid crs = do
      void $ raftFork (CustomThreadRole "Respond to Client") $ do
        clientResp <- mkClientResp crs
        -- TODO log failure if sendClient fails
        lift (sendClient cid clientResp)

    mkClientResp :: ClientRespSpec sm v -> RaftT sm v m (ClientResponse sm v)
    mkClientResp crs =
      case crs of
        ClientReadRespSpec crrs ->
          ClientReadResponse <$>
            case crrs of
              ClientReadRespSpecEntries res -> do
                eRes <- lift (readEntries res)
                case eRes of
                  Left err -> throwM err
                  Right res ->
                    case res of
                      OneEntry e -> pure (ClientReadRespEntry e)
                      ManyEntries es -> pure (ClientReadRespEntries es)
              ClientReadRespSpecStateMachine sm ->
                pure (ClientReadRespStateMachine sm)
        ClientWriteRespSpec cwrs ->
          ClientWriteResponse <$>
            case cwrs of
              ClientWriteRespSpecSuccess idx sn ->
                pure (ClientWriteRespSuccess idx sn)
              ClientWriteRespSpecFail sn err ->
                pure (ClientWriteRespFail sn err)
        ClientRedirRespSpec cl ->
          pure (ClientRedirectResponse (ClientRedirResp cl))
        ClientMetricsRespSpec rnm ->
          pure (ClientMetricsResponse (ClientMetricsResp rnm))

    mkRPCfromSendRPCAction :: SendRPCAction v -> RaftT sm v m (RPCMessage v)
    mkRPCfromSendRPCAction sendRPCAction = do
      RaftNodeState ns <- get
      nodeConfig <- asks raftNodeConfig
      RPCMessage (raftConfigNodeId nodeConfig) <$>
        case sendRPCAction of
          SendAppendEntriesRPC aeData -> do
            (entries, prevLogIndex, prevLogTerm, aeReadReq) <-
              case aedEntriesSpec aeData of
                FromIndex idx -> attachNothing <$> mkPrevLogEntryDataByIdx idx
                FromClientWriteReq e -> attachNothing <$> mkPrevEntryDataByEntry e
                FromNewLeader e -> attachNothing <$> mkPrevEntryDataByEntry e
                NoEntries spec -> do
                  let readReq =
                        case spec of
                          FromClientReadReq n -> Just n
                          FromHeartbeat -> Nothing
                  (prevLogIdx, prevLogTerm) <-
                    case getLastLogEntry ns of
                      NoLogEntries -> pure (index0, term0)
                      LastLogEntry e -> do
                        (_,prevLogIdx, prevLogTerm) <- mkPrevEntryDataByEntry e
                        pure (prevLogIdx, prevLogTerm)
                  pure (Empty, prevLogIdx, prevLogTerm, readReq)
            let leaderId = LeaderId (raftConfigNodeId nodeConfig)
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

    attachNothing (x,y,z) = (x,y,z,Nothing)

    -- Make the previous log entry data given an index of an entry in the log.
    -- This function is used for creating heartbeat RPCs, where all logs from
    -- a particular index onwards are sent to a follower.
    mkPrevLogEntryDataByIdx idx = do
      eLogEntries <- lift (readLogEntriesFrom (decrIndexWithDefault0 idx))
      case eLogEntries of
        Left err -> throwM err
        Right log ->
          case log of
            pe :<| entries@(e :<| _)
              | idx == 1 -> pure (log, index0, term0)
              | otherwise -> pure (entries, entryIndex pe, entryTerm pe)
            _ -> pure (log, index0, term0)

    -- Make the previous log entry data given a specific log entry and return
    -- the list of entries equal to the singleton including the original entry.
    -- This function is used for creating Append Entry RPCs resulting from
    -- client write requests, new leader no-op entries, empty heartbeat RPCs,
    -- and client read requests.
    mkPrevEntryDataByEntry e
      | entryIndex e == Index 1 = pure (singleton e, index0, term0)
      | otherwise = do
          let prevLogEntryIdx = decrIndexWithDefault0 (entryIndex e)
          eLogEntry <- lift $ readLogEntry prevLogEntryIdx
          case eLogEntry of
            Left err -> throwM err
            Right Nothing -> pure (singleton e, index0, term0)
            Right (Just (prevEntry :: Entry v)) ->
              pure (singleton e, entryIndex prevEntry, entryTerm prevEntry)

-- If commitIndex > lastApplied: increment lastApplied, apply
-- log[lastApplied] to state machine (Section 5.3) until the state machine
-- is up to date with all the committed log entries
applyLogEntries
  :: forall sm m v.
     ( Show v, Show sm, Show (RaftStateMachinePureError sm v)
     , MonadIO m, MonadThrow m, MonadRaft v m
     , RaftReadLog m v, Exception (RaftReadLogError m)
     , RaftStateMachine m sm v
     )
  => sm
  -> RaftT sm v m sm
applyLogEntries stateMachine = do
    raftNodeState@(RaftNodeState nodeState) <- get
    if commitIndex nodeState > lastApplied nodeState
      then do
        let resNodeState = incrLastApplied nodeState
        put $ RaftNodeState resNodeState
        let newLastAppliedIndex = lastApplied resNodeState
        eLogEntry <- lift $ readLogEntry newLastAppliedIndex
        case eLogEntry of
          Left err -> throwM err
          Right Nothing ->
            logAndPanic . mconcat $
              [ "No log entry at 'newLastAppliedIndex := "
              , show newLastAppliedIndex <> "'"
              ]
          Right (Just logEntry) -> do
            -- The command should be verified by the leader, thus all node
            -- attempting to apply the committed log entry should not fail when
            -- doing so; failure here means something has gone very wrong.
            logDebug $ "[Applying Log Entry]: " <> show logEntry
            eRes <- lift (applyLogEntry NoMonadicValidation stateMachine logEntry)
            case eRes of
              Left err -> logAndPanic $ "Failed to apply committed log entry: " <> show err
              Right nsm -> applyLogEntries nsm
      else pure stateMachine
  where
    incrLastApplied :: NodeState ns sm v -> NodeState ns sm v
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

    lastApplied :: NodeState ns sm v -> Index
    lastApplied = fst . getLastAppliedAndCommitIndex

    commitIndex :: NodeState ns sm v -> Index
    commitIndex = snd . getLastAppliedAndCommitIndex

handleLogs
  :: (MonadIO m, MonadRaft v m)
  => [LogMsg]
  -> RaftT sm v m ()
handleLogs logs = do
  logCtx <- asks raftNodeLogCtx
  mapM_ (logToDest logCtx) logs

------------------------------------------------------------------------------
-- Event Producers
------------------------------------------------------------------------------

-- | Producer for rpc message events
rpcHandler
  :: (MonadIO m, MonadRaft v m, MonadCatch m, Show v, RaftRecvRPC m v)
  => RaftT sm v m ()
rpcHandler =
  forever $ do
    eRpcMsg <- lift $ Control.Monad.Catch.try receiveRPC
    case eRpcMsg of
      Left (err :: SomeException) -> logCritical (show err)
      Right (Left err) -> logCritical (show err)
      Right (Right rpcMsg) -> do
        let rpcMsgEvent = MessageEvent (RPCMessageEvent rpcMsg)
        writeRaftEventChan rpcMsgEvent

-- | Producer for rpc message events
clientReqHandler
  :: (MonadIO m, MonadRaft v m, MonadCatch m, RaftRecvClient m v)
  => RaftT sm v m ()
clientReqHandler =
  forever $ do
    eClientReq <- lift $ Control.Monad.Catch.try receiveClient
    case eClientReq of
      Left (err :: SomeException) -> logCritical (show err)
      Right (Left err) -> logCritical (show err)
      Right (Right clientReq) -> do
        let clientReqEvent = MessageEvent (ClientRequestEvent clientReq)
        writeRaftEventChan clientReqEvent

-- | Producer for the election timeout event
electionTimeoutTimer :: forall sm v m. (MonadIO m, MonadRaft v m) => Timer -> RaftT sm v m ()
electionTimeoutTimer timer =
  forever $ do
    success <- liftIO $ startTimer timer
    when (not success) $
      panic "[Failed invariant]: Election timeout timer failed to start."
    liftIO $ waitTimer timer
    writeTimeoutEvent ElectionTimeout

-- | Producer for the heartbeat timeout event
heartbeatTimeoutTimer :: forall sm v m. (MonadIO m, MonadRaft v m) => Timer -> RaftT sm v m ()
heartbeatTimeoutTimer timer =
  forever $ do
    success <- liftIO $ startTimer timer
    when (not success) $
      panic "[Failed invariant]: Heartbeat timeout timer failed to start."
    liftIO $ waitTimer timer
    writeTimeoutEvent HeartbeatTimeout

writeTimeoutEvent :: forall sm v m. (MonadIO m , MonadRaft v m) => Timeout -> RaftT sm v m ()
writeTimeoutEvent timeout = do
  now <- liftIO getSystemTime
  writeRaftEventChan (TimeoutEvent now timeout)
