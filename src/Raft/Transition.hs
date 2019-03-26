{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeFamilyDependencies #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE GADTs #-}

module Raft.Transition where

import Protolude hiding (pass)

import Control.Arrow ((&&&))
import Control.Monad.RWS.Strict

import qualified Data.Set as Set

import Raft.Action
import Raft.Client
import Raft.Config
import Raft.Event
import Raft.Log
import Raft.Persistent
import Raft.Metrics (RaftNodeMetrics)
import Raft.NodeState
import Raft.RPC
import Raft.Types
import Raft.Logging (RaftLogger, runRaftLoggerT, RaftLoggerT(..), LogMsg)
import qualified Raft.Logging as Logging


--------------------------------------------------------------------------------
-- Raft Transition Monad
--------------------------------------------------------------------------------

tellAction :: Action sm v -> TransitionM sm v ()
tellAction a = tell [a]

tellActions :: [Action sm v] -> TransitionM sm v ()
tellActions as = tell as

data TransitionEnv sm v = TransitionEnv
  { nodeConfig :: RaftNodeConfig
  , stateMachine :: sm
  , nodeState :: RaftNodeState sm v
  , nodeMetrics :: RaftNodeMetrics
  }

newtype TransitionM sm v a = TransitionM
  { unTransitionM :: RaftLoggerT sm v (RWS (TransitionEnv sm v) [Action sm v] PersistentState) a
  } deriving (Functor, Applicative, Monad)

instance MonadWriter [Action sm v] (TransitionM sm v) where
  tell = TransitionM . RaftLoggerT . tell
  listen = TransitionM . RaftLoggerT . listen . unRaftLoggerT . unTransitionM
  pass = TransitionM . RaftLoggerT . pass . unRaftLoggerT . unTransitionM

instance MonadReader (TransitionEnv sm v) (TransitionM sm v) where
  ask = TransitionM . RaftLoggerT $ ask
  local f = TransitionM . RaftLoggerT . local f . unRaftLoggerT . unTransitionM

instance MonadState PersistentState (TransitionM sm v) where
  get = TransitionM . RaftLoggerT $ lift get
  put = TransitionM . RaftLoggerT . lift . put

instance RaftLogger sm v (RWS (TransitionEnv sm v) [Action sm v] PersistentState) where
  loggerCtx = asks ((raftConfigNodeId . nodeConfig) &&& nodeState)

runTransitionM
  :: TransitionEnv sm v
  -> PersistentState
  -> TransitionM sm v a
  -> ((a, [LogMsg]), PersistentState, [Action sm v])
runTransitionM transEnv persistentState transitionM =
  runRWS (runRaftLoggerT (unTransitionM transitionM)) transEnv persistentState

askNodeId :: TransitionM sm v NodeId
askNodeId = asks (raftConfigNodeId . nodeConfig)

-- | Returns the set of all node ids excluding the node's own id
askPeerNodeIds :: TransitionM sm v NodeIds
askPeerNodeIds = do
  selfNodeId <- askNodeId
  allNodeIds <- asks (raftConfigNodeIds . nodeConfig)
  pure (Set.delete selfNodeId allNodeIds)

--------------------------------------------------------------------------------
-- Handlers
--------------------------------------------------------------------------------

type RPCHandler ns sm r v = (RPCType r v, Show sm, Show v) => NodeState ns sm v -> NodeId -> r -> TransitionM sm v (ResultState ns sm v)
type TimeoutHandler ns sm v = (Show sm, Show v) => NodeState ns sm v -> Timeout -> TransitionM sm v (ResultState ns sm v)
type ClientReqHandler ns cr sm v = (ClientReqType cr v, Show sm, Show v) => NodeState ns sm v -> ClientId -> cr -> TransitionM sm v (ResultState ns sm v)

--------------------------------------------------------------------------------
-- RWS Helpers
--------------------------------------------------------------------------------

broadcast :: SendRPCAction v -> TransitionM sm v ()
broadcast sendRPC = do
  selfNodeId <- askNodeId
  tellAction =<<
    flip BroadcastRPC sendRPC
      <$> asks (Set.filter (selfNodeId /=) . raftConfigNodeIds . nodeConfig)

send :: NodeId -> SendRPCAction v -> TransitionM sm v ()
send nodeId sendRPC = tellAction (SendRPC nodeId sendRPC)

-- | Resets the election timeout.
resetElectionTimeout :: TransitionM sm v ()
resetElectionTimeout = tellAction (ResetTimeoutTimer ElectionTimeout)

resetHeartbeatTimeout :: TransitionM sm v ()
resetHeartbeatTimeout = tellAction (ResetTimeoutTimer HeartbeatTimeout)

redirectClientToLeader :: ClientId -> CurrentLeader -> TransitionM sm v ()
redirectClientToLeader clientId currentLeader = do
  let clientRedirRespSpec = ClientRedirRespSpec currentLeader
  tellAction (RespondToClient clientId clientRedirRespSpec)

respondClientRead :: ClientId -> ClientReadReq -> TransitionM sm v ()
respondClientRead clientId readReq = do
  readReqData <-
    case readReq of
      ClientReadEntries res -> pure (ClientReadRespSpecEntries res)
      ClientReadStateMachine -> do
        sm <- asks stateMachine
        pure (ClientReadRespSpecStateMachine sm)
  tellAction . RespondToClient clientId . ClientReadRespSpec $ readReqData

respondClientWrite :: ClientId -> Index -> SerialNum -> TransitionM sm v ()
respondClientWrite cid entryIdx sn =
  tellAction (RespondToClient cid (ClientWriteRespSpec (ClientWriteRespSpecSuccess entryIdx sn)))

respondClientMetrics :: ClientId -> TransitionM sm v ()
respondClientMetrics cid =
  tellAction . RespondToClient cid . ClientMetricsRespSpec =<< asks nodeMetrics

respondClientRedir :: ClientId -> CurrentLeader -> TransitionM sm v ()
respondClientRedir cid cl =
  tellAction (RespondToClient cid (ClientRedirRespSpec cl))

appendLogEntries :: Show v => Seq (Entry v) -> TransitionM sm v ()
appendLogEntries = tellAction . AppendLogEntries

updateClientReqCacheFromIdx :: Index -> TransitionM sm v ()
updateClientReqCacheFromIdx = tellAction . UpdateClientReqCacheFrom

--------------------------------------------------------------------------------

startElection
  :: Index
  -> Index
  -> LastLogEntry v
  -> ClientWriteReqCache
  -> TransitionM sm v (CandidateState sm v)
startElection commitIndex lastApplied lastLogEntry clientReqCache  = do
    incrementTerm
    voteForSelf
    resetElectionTimeout
    broadcast =<< requestVoteMessage
    selfNodeId <- askNodeId
    -- Return new candidate state
    pure CandidateState
      { csCommitIndex = commitIndex
      , csLastApplied = lastApplied
      , csVotes = Set.singleton selfNodeId
      , csLastLogEntry = lastLogEntry
      , csClientReqCache = clientReqCache
      }
  where
    requestVoteMessage = do
      term <- currentTerm <$> get
      selfNodeId <- askNodeId
      pure $ SendRequestVoteRPC
        RequestVote
          { rvTerm = term
          , rvCandidateId = selfNodeId
          , rvLastLogIndex = lastLogEntryIndex lastLogEntry
          , rvLastLogTerm = lastLogEntryTerm lastLogEntry
          }

    incrementTerm = do
      psNextTerm <- incrTerm . currentTerm <$> get
      modify $ \pstate ->
        pstate { currentTerm = psNextTerm
               , votedFor = Nothing
               }

    voteForSelf = do
      selfNodeId <- askNodeId
      modify $ \pstate ->
        pstate { votedFor = Just selfNodeId }

--------------------------------------------------------------------------------
-- Logging
--------------------------------------------------------------------------------

logInfo = TransitionM . Logging.logInfo
logDebug = TransitionM . Logging.logDebug
