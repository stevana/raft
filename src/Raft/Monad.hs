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

module Raft.Monad where

import Protolude hiding (pass)

import Control.Arrow ((&&&))
import Control.Monad.RWS

import qualified Data.Set as Set

import Raft.Action
import Raft.Client
import Raft.Config
import Raft.Event
import Raft.Log
import Raft.Persistent
import Raft.NodeState
import Raft.RPC
import Raft.Types
import Raft.Logging (RaftLogger, runRaftLoggerT, RaftLoggerT(..), LogMsg)
import qualified Raft.Logging as Logging

--------------------------------------------------------------------------------
-- State Machine
--------------------------------------------------------------------------------

-- | Interface to handle commands in the underlying state machine. Functional
--dependency permitting only a single state machine command to be defined to
--update the state machine.

class RSMP sm v | sm -> v where
  data RSMPError sm v
  type RSMPCtx sm v = ctx | ctx -> sm v
  applyCmdRSMP :: RSMPCtx sm v -> sm -> v -> Either (RSMPError sm v) sm

class (Monad m, RSMP sm v) => RSM sm v m | m sm -> v where
  validateCmd :: v -> m (Either (RSMPError sm v) ())
  askRSMPCtx :: m (RSMPCtx sm v)

applyEntryRSM :: RSM sm v m => sm -> Entry v -> m (Either (RSMPError sm v) sm)
applyEntryRSM sm e  =
  case entryValue e of
    NoValue -> pure (Right sm)
    EntryValue v -> do
      res <- validateCmd v
      case res of
        Left err -> pure (Left err)
        Right () -> do
          ctx <- askRSMPCtx
          pure (applyCmdRSMP ctx sm v)

--------------------------------------------------------------------------------
-- Raft Monad
--------------------------------------------------------------------------------

tellAction :: Action sm v -> TransitionM sm v ()
tellAction a = tell [a]

tellActions :: [Action sm v] -> TransitionM sm v ()
tellActions as = tell as

data TransitionEnv sm v = TransitionEnv
  { nodeConfig :: NodeConfig
  , stateMachine :: sm
  , nodeState :: RaftNodeState v
  }

newtype TransitionM sm v a = TransitionM
  { unTransitionM :: RaftLoggerT v (RWS (TransitionEnv sm v) [Action sm v] PersistentState) a
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

instance RaftLogger v (RWS (TransitionEnv sm v) [Action sm v] PersistentState) where
  loggerCtx = asks ((configNodeId . nodeConfig) &&& nodeState)

runTransitionM
  :: TransitionEnv sm v
  -> PersistentState
  -> TransitionM sm v a
  -> ((a, [LogMsg]), PersistentState, [Action sm v])
runTransitionM transEnv persistentState transitionM =
  runRWS (runRaftLoggerT (unTransitionM transitionM)) transEnv persistentState

askNodeId :: TransitionM sm v NodeId
askNodeId = asks (configNodeId . nodeConfig)

--------------------------------------------------------------------------------
-- Handlers
--------------------------------------------------------------------------------

type RPCHandler ns sm r v = (RPCType r v, Show v) => NodeState ns v -> NodeId -> r -> TransitionM sm v (ResultState ns v)
type TimeoutHandler ns sm v = Show v => NodeState ns v -> Timeout -> TransitionM sm v (ResultState ns v)
type ClientReqHandler ns sm v = Show v => NodeState ns v -> ClientRequest v -> TransitionM sm v (ResultState ns v)

--------------------------------------------------------------------------------
-- RWS Helpers
--------------------------------------------------------------------------------

broadcast :: SendRPCAction v -> TransitionM sm v ()
broadcast sendRPC = do
  selfNodeId <- askNodeId
  tellAction =<<
    flip BroadcastRPC sendRPC
      <$> asks (Set.filter (selfNodeId /=) . configNodeIds . nodeConfig)

send :: NodeId -> SendRPCAction v -> TransitionM sm v ()
send nodeId sendRPC = tellAction (SendRPC nodeId sendRPC)

-- | Resets the election timeout.
resetElectionTimeout :: TransitionM sm v ()
resetElectionTimeout = tellAction (ResetTimeoutTimer ElectionTimeout)

resetHeartbeatTimeout :: TransitionM sm v ()
resetHeartbeatTimeout = tellAction (ResetTimeoutTimer HeartbeatTimeout)

redirectClientToLeader :: ClientId -> CurrentLeader -> TransitionM sm v ()
redirectClientToLeader clientId currentLeader = do
  let clientRedirResp = ClientRedirectResponse (ClientRedirResp currentLeader)
  tellAction (RespondToClient clientId clientRedirResp)

respondClientRead :: ClientId -> TransitionM sm v ()
respondClientRead clientId = do
  clientReadResp <- ClientReadResponse . ClientReadResp <$> asks stateMachine
  tellAction (RespondToClient clientId clientReadResp)

respondClientWrite :: ClientId -> Index -> SerialNum -> TransitionM sm v ()
respondClientWrite cid entryIdx sn =
  tellActions [RespondToClient cid (ClientWriteResponse (ClientWriteResp entryIdx sn))]

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
  -> TransitionM sm v (CandidateState v)
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
