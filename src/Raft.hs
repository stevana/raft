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


-- | Run timers, RPC and client request handlers and start event loop.
-- It should run forever
runRaftNode
  :: forall m sm v.
     ( Typeable m, Show v, Show sm, Serialize v, Show (Action sm v), Show (RaftLogError m), Show (RaftStateMachinePureError sm v)
     , MonadIO m, MonadCatch m, MonadFail m, MonadRaft v m
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
   => RaftNodeConfig       -- ^ Node configuration
   -> LogCtx (RaftT v m)   -- ^ Logs destination
   -> Int                  -- ^ Timer seed
   -> sm                   -- ^ Initial state machine state
   -> m ()
runRaftNode nodeConfig@RaftNodeConfig{..} logCtx timerSeed initRaftStateMachine = do
  -- Initialize the persistent state and logs storage if specified
  initializeStorage

  eventChan <- newRaftChan @v

  -- Create timers and reset timer actions
  electionTimer <- liftIO $ newTimerRange timerSeed configElectionTimeout
  heartbeatTimer <- liftIO $ newTimer configHeartbeatTimeout
  let resetElectionTimer = liftIO $ void $ resetTimer electionTimer
      resetHeartbeatTimer = liftIO $ void $ resetTimer heartbeatTimer

  let raftEnv = RaftEnv eventChan resetElectionTimer resetHeartbeatTimer nodeConfig logCtx
  runRaftT initRaftNodeState raftEnv $ do

    -- These event producers need access to logging, thus they live in RaftT
    --
    -- Note: Changing the roles of these event producers (the strings passed as
    -- arguments to 'raftFork' should incur a minor (or major?) version bump,
    -- because some implementations of 'MonadRaftFork' rely on these strings to
    -- reliably send messages to the event producers (e.g. cloud-haskell
    -- processes).
    raftFork (CustomThreadRole "Election Timeout Timer") . lift $
      electionTimeoutTimer @m @v eventChan electionTimer
    raftFork (CustomThreadRole "Heartbeat Timeout Timer") . lift $
      heartbeatTimeoutTimer @m @v eventChan heartbeatTimer
    raftFork RPCHandler (rpcHandler @m @v eventChan)
    raftFork ClientRequestHandler (clientReqHandler @m @v eventChan)

    -- Start the main event handling loop
    handleEventLoop initRaftStateMachine

  where
    initializeStorage =
      case configStorageState of
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
     , MonadIO m, MonadRaft v m, MonadFail m, MonadThrow m
     , RaftStateMachine m sm v
     , Show (RaftStateMachinePureError sm v)
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
handleEventLoop initRaftStateMachine = do
    setInitLastLogEntry
    ePersistentState <- lift readPersistentState
    case ePersistentState of
      Left err -> throwM err
      Right pstate -> handleEventLoop' initRaftStateMachine pstate
  where

    -- Some events must be validated before being processed by the system:
    --
    --   - If the node is a leader, and the event is a client write request, the
    --   command issued by the client must satisfy a predicate given by the
    --   programmer in the 'RaftStateMachine` and 'RaftStateMachinePure' typeclasses
    --
    -- This function returns 'Nothing' if the event was not valid, and returns
    -- the result of the continuation function wrapped in 'Just', otherwise.
    withValidatedEvent :: sm -> (Event v -> RaftT v m a) -> RaftT v m (Maybe a)
    withValidatedEvent stateMachine f = do
      event <- lift . readRaftChan =<< asks eventChan
      RaftNodeState raftNodeState <- get
      case raftNodeState of
        NodeLeaderState _ -> do
          case event of
            MessageEvent (ClientRequestEvent (ClientRequest cid creq)) ->
              case creq of
                ClientWriteReq serial cmd -> do
                  eRes <- lift (applyLogCmd MonadicValidation stateMachine cmd)
                  case eRes of
                    Left err -> do
                      let clientWriteRespSpec = ClientWriteRespSpec (ClientWriteRespSpecFail serial err)
                          clientFailRespAction = RespondToClient cid clientWriteRespSpec
                      handleAction clientFailRespAction
                      pure Nothing
                    -- Don't actually do anything if validating the command succeeds
                    Right _ -> Just <$> f event
                _ -> Just <$> f event
            _ -> Just <$> f event
        _ -> Just <$> f event

    handleEventLoop' :: sm -> PersistentState -> RaftT v m ()
    handleEventLoop' stateMachine persistentState = do

      mRes <-
        withValidatedEvent stateMachine $ \event -> do
          loadLogEntryTermAtAePrevLogIndex event
          raftNodeState <- get
          logDebug $ "[Event]: " <> show event
          logDebug $ "[NodeState]: " <> show raftNodeState
          logDebug $ "[State Machine]: " <> show stateMachine
          logDebug $ "[Persistent State]: " <> show persistentState

          -- Perform core state machine transition, handling the current event
          nodeConfig <- asks raftNodeConfig
          let transitionEnv = TransitionEnv nodeConfig stateMachine raftNodeState
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
    loadLogEntryTermAtAePrevLogIndex :: Event v -> RaftT v m ()
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
    setInitLastLogEntry :: RaftT v m ()
    setInitLastLogEntry = do
      RaftNodeState rns <- get
      eLogEntry <- lift readLastLogEntry
      case eLogEntry of
        Left err -> throwM err
        Right Nothing -> pure ()
        Right (Just e) ->
          put (RaftNodeState (setLastLogEntry rns (singleton e)))

handleActions
  :: ( Show v, Show sm, Show (Action sm v), Show (RaftLogError m), Typeable m
     , MonadIO m, MonadRaft v m, MonadThrow m
     , RaftStateMachine m sm v
     , RaftSendRPC m v
     , RaftSendClient m sm v
     , RaftLog m v
     , RaftLogExceptions m
     )
  => [Action sm v]
  -> RaftT v m ()
handleActions actions =
  mapM_ handleAction actions

handleAction
  :: forall sm v m.
     ( Show v, Show sm, Show (Action sm v), Show (RaftLogError m), Typeable m
     , MonadIO m, MonadRaft v m, MonadThrow m
     , RaftStateMachine m sm v
     , RaftSendRPC m v
     , RaftSendClient m sm v
     , RaftLog m v
     , RaftLogExceptions m
     )
  => Action sm v
  -> RaftT v m ()
handleAction action = do
  logDebug $ "Handling [Action]: " <> show action
  case action of
    SendRPC nid sendRpcAction -> do
      rpcMsg <- mkRPCfromSendRPCAction sendRpcAction
      lift (sendRPC nid rpcMsg)
    SendRPCs rpcMap ->
      flip mapM_ (Map.toList rpcMap) $ \(nid, sendRpcAction) ->
        raftFork (CustomThreadRole "Send RPC") $ do
          rpcMsg <- mkRPCfromSendRPCAction sendRpcAction
          lift (sendRPC nid rpcMsg)
    BroadcastRPC nids sendRpcAction -> do
      rpcMsg <- mkRPCfromSendRPCAction sendRpcAction
      mapM_ (raftFork (CustomThreadRole "RPC Broadcast Thread") . lift . flip sendRPC rpcMsg) nids
    RespondToClient cid cr -> respondToClient cid cr
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
    respondToClientWrite :: (ClientId, (SerialNum, Index)) -> RaftT v m ()
    respondToClientWrite (cid, (sn,idx)) = do
      let clientWriteRespSpec =
            ClientWriteRespSpec @sm (ClientWriteRespSpecSuccess idx sn)
      respondToClient cid clientWriteRespSpec

    respondToClient :: ClientId -> ClientRespSpec sm v -> RaftT v m ()
    respondToClient cid crs = do
      clientResp <- mkClientResp crs
      -- TODO log failure if sendClient fails
      void $ raftFork (CustomThreadRole "Respond to Client") $
        lift (sendClient cid clientResp)

    mkClientResp :: ClientRespSpec sm v -> RaftT v m (ClientResponse sm v)
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

    mkRPCfromSendRPCAction :: SendRPCAction v -> RaftT v m (RPCMessage v)
    mkRPCfromSendRPCAction sendRPCAction = do
      RaftNodeState ns <- get
      nodeConfig <- asks raftNodeConfig
      RPCMessage (configNodeId nodeConfig) <$>
        case sendRPCAction of
          SendAppendEntriesRPC aeData -> do
            (entries, prevLogIndex, prevLogTerm, aeReadReq) <-
              case aedEntriesSpec aeData of
                FromIndex idx -> do
                  eLogEntries <- lift (readLogEntriesFrom (decrIndexWithDefault0 idx))
                  case eLogEntries of
                    Left err -> throwM err
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
          Left err -> throwM err
          Right Nothing -> logAndPanic $ "No log entry at 'newLastAppliedIndex := " <> show newLastAppliedIndex <> "'"
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
  :: (MonadIO m, MonadRaft v m)
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
  :: forall m v. (MonadIO m, MonadRaft v m, MonadCatch m, Show v, RaftRecvRPC m v)
  => RaftEventChan v m
  -> RaftT v m ()
rpcHandler eventChan =
  forever $ do
    eRpcMsg <- lift $ Control.Monad.Catch.try receiveRPC
    case eRpcMsg of
      Left (err :: SomeException) -> logCritical (show err)
      Right (Left err) -> logCritical (show err)
      Right (Right rpcMsg) -> do
        let rpcMsgEvent = MessageEvent (RPCMessageEvent rpcMsg)
        lift $ writeRaftChan @v @m eventChan rpcMsgEvent

-- | Producer for rpc message events
clientReqHandler
  :: forall m v. (MonadIO m, MonadRaft v m, MonadCatch m, RaftRecvClient m v)
  => RaftEventChan v m
  -> RaftT v m ()
clientReqHandler eventChan =
  forever $ do
    eClientReq <- lift $ Control.Monad.Catch.try receiveClient
    case eClientReq of
      Left (err :: SomeException) -> logCritical (show err)
      Right (Left err) -> logCritical (show err)
      Right (Right clientReq) -> do
        let clientReqEvent = MessageEvent (ClientRequestEvent clientReq)
        lift $ writeRaftChan @v @m eventChan clientReqEvent

-- | Producer for the election timeout event
electionTimeoutTimer :: forall m v. (MonadIO m, MonadRaft v m) => RaftEventChan v m -> Timer -> m ()
electionTimeoutTimer eventChan timer =
  forever $ do
    success <- liftIO $ startTimer timer
    when (not success) $
      panic "[Failed invariant]: Election timeout timer failed to start."
    liftIO $ waitTimer timer
    writeTimeoutEvent @m @v eventChan ElectionTimeout

-- | Producer for the heartbeat timeout event
heartbeatTimeoutTimer :: forall m v. (MonadIO m, MonadRaft v m) => RaftEventChan v m -> Timer -> m ()
heartbeatTimeoutTimer eventChan timer =
  forever $ do
    success <- liftIO $ startTimer timer
    when (not success) $
      panic "[Failed invariant]: Heartbeat timeout timer failed to start."
    liftIO $ waitTimer timer
    writeTimeoutEvent @m @v eventChan HeartbeatTimeout

writeTimeoutEvent :: forall m v. (MonadIO m , MonadRaft v m) => RaftEventChan v m -> Timeout -> m ()
writeTimeoutEvent eventChan timeout = do
  now <- liftIO getSystemTime
  writeRaftChan @v @m eventChan (TimeoutEvent now timeout)
