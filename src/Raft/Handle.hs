{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE GADTs #-}

module Raft.Handle where

import Protolude

import Data.Serialize (Serialize)

import qualified Raft.Follower as Follower
import qualified Raft.Candidate as Candidate
import qualified Raft.Leader as Leader

import Raft.Action
import Raft.Event
import Raft.Client
import Raft.Transition
import Raft.NodeState
import Raft.Persistent
import Raft.RPC
import Raft.StateMachine
import Raft.Types
import Raft.Logging (LogMsg)

-- | Main entry point for handling events
handleEvent
  :: forall sm v.
     (RaftStateMachinePure sm v, Show sm, Show v, Serialize v)
  => RaftNodeState sm v
  -> TransitionEnv sm v
  -> PersistentState
  -> Event v
  -> (RaftNodeState sm v, PersistentState, [Action sm v], [LogMsg])
handleEvent raftNodeState@(RaftNodeState initNodeState) transitionEnv persistentState event =
    -- Rules for all servers:
    case handleNewerRPCTerm of
      ((RaftNodeState resNodeState, logMsgs), persistentState', outputs) ->
        case handleEvent' resNodeState transitionEnv persistentState' event of
          ((ResultState transition resultState, logMsgs'), persistentState'', outputs') ->
            (RaftNodeState resultState, persistentState'', outputs <> outputs', logMsgs <> logMsgs')
  where
    handleNewerRPCTerm :: ((RaftNodeState sm v, [LogMsg]), PersistentState, [Action sm v])
    handleNewerRPCTerm =
      case event of
        MessageEvent (RPCMessageEvent (RPCMessage _ rpc)) ->
          runTransitionM transitionEnv persistentState $ do
            -- If RPC request or response contains term T > currentTerm: set
            -- currentTerm = T, convert to follower
            currentTerm <- gets currentTerm
            if (currentTerm < rpcTerm rpc)
              then convertToFollower (rpcTerm rpc)
              else pure raftNodeState
        _ -> ((raftNodeState, []), persistentState, mempty)

    convertToFollower :: Term -> TransitionM sm v (RaftNodeState sm v)
    convertToFollower term =
      case convertToFollowerState initNodeState of
        ResultState _ nodeState -> do
          modify $ \pstate ->
            pstate { currentTerm = term
                   , votedFor = Nothing
                   }
          resetElectionTimeout
          pure (RaftNodeState nodeState)

    convertToFollowerState :: forall ns. NodeState ns sm v -> ResultState ns sm v
    convertToFollowerState nodeState =
      case nodeState of
        NodeFollowerState _ ->
          ResultState HigherTermFoundFollower nodeState
        NodeCandidateState cs ->
          ResultState HigherTermFoundCandidate $
            NodeFollowerState FollowerState
              { fsCurrentLeader = NoLeader
              , fsCommitIndex = csCommitIndex cs
              , fsLastApplied = csLastApplied cs
              , fsLastLogEntry = csLastLogEntry cs
              , fsTermAtAEPrevIndex = Nothing
              , fsClientReqCache = csClientReqCache cs
              }
        NodeLeaderState ls ->
          ResultState HigherTermFoundLeader $
            NodeFollowerState FollowerState
              { fsCurrentLeader = NoLeader
              , fsCommitIndex = lsCommitIndex ls
              , fsLastApplied = lsLastApplied ls
              , fsLastLogEntry = lsLastLogEntry ls
              , fsTermAtAEPrevIndex = Nothing
              , fsClientReqCache = lsClientReqCache ls
              }


data RaftHandler ns sm v = RaftHandler
  { handleAppendEntries :: RPCHandler ns sm (AppendEntries v) v
  , handleAppendEntriesResponse :: RPCHandler ns sm AppendEntriesResponse v
  , handleRequestVote :: RPCHandler ns sm RequestVote v
  , handleRequestVoteResponse :: RPCHandler ns sm RequestVoteResponse v
  , handleTimeout :: TimeoutHandler ns sm v
  , handleClientReadRequest :: ClientReqHandler ns ClientReadReq sm v
  , handleClientWriteRequest :: ClientReqHandler ns (ClientWriteReq v) sm v
  }

followerRaftHandler :: (Show v, Serialize v) => RaftHandler 'Follower sm v
followerRaftHandler = RaftHandler
  { handleAppendEntries = Follower.handleAppendEntries
  , handleAppendEntriesResponse = Follower.handleAppendEntriesResponse
  , handleRequestVote = Follower.handleRequestVote
  , handleRequestVoteResponse = Follower.handleRequestVoteResponse
  , handleTimeout = Follower.handleTimeout
  , handleClientReadRequest = Follower.handleClientReadRequest
  , handleClientWriteRequest = Follower.handleClientWriteRequest
  }

candidateRaftHandler :: (Show v, Serialize v) => RaftHandler 'Candidate sm v
candidateRaftHandler = RaftHandler
  { handleAppendEntries = Candidate.handleAppendEntries
  , handleAppendEntriesResponse = Candidate.handleAppendEntriesResponse
  , handleRequestVote = Candidate.handleRequestVote
  , handleRequestVoteResponse = Candidate.handleRequestVoteResponse
  , handleTimeout = Candidate.handleTimeout
  , handleClientReadRequest = Candidate.handleClientReadRequest
  , handleClientWriteRequest = Candidate.handleClientWriteRequest
  }

leaderRaftHandler :: (Show v, Serialize v) => RaftHandler 'Leader sm v
leaderRaftHandler = RaftHandler
  { handleAppendEntries = Leader.handleAppendEntries
  , handleAppendEntriesResponse = Leader.handleAppendEntriesResponse
  , handleRequestVote = Leader.handleRequestVote
  , handleRequestVoteResponse = Leader.handleRequestVoteResponse
  , handleTimeout = Leader.handleTimeout
  , handleClientReadRequest = Leader.handleClientReadRequest
  , handleClientWriteRequest = Leader.handleClientWriteRequest
  }

mkRaftHandler :: forall ns sm v. (Show v, Serialize v) => NodeState ns sm v -> RaftHandler ns sm v
mkRaftHandler nodeState =
  case nodeState of
    NodeFollowerState _ -> followerRaftHandler
    NodeCandidateState _ -> candidateRaftHandler
    NodeLeaderState _ -> leaderRaftHandler

handleEvent'
  :: forall ns sm v.
     (RaftStateMachinePure sm v, Show sm, Show v, Serialize v)
  => NodeState ns sm v
  -> TransitionEnv sm v
  -> PersistentState
  -> Event v
  -> ((ResultState ns sm v, [LogMsg]), PersistentState, [Action sm v])
handleEvent' initNodeState transitionEnv persistentState event =
    runTransitionM transitionEnv persistentState $ do
      case event of
        MessageEvent mev ->
          case mev of
            RPCMessageEvent rpcMsg -> handleRPCMessage rpcMsg
            ClientRequestEvent clientReq -> handleClientRequest clientReq
        TimeoutEvent _ tout -> handleTimeout initNodeState tout
  where
    RaftHandler{..} = mkRaftHandler initNodeState

    handleRPCMessage :: RPCMessage v -> TransitionM sm v (ResultState ns sm v)
    handleRPCMessage (RPCMessage sender rpc) =
      case rpc of
        AppendEntriesRPC appendEntries ->
          handleAppendEntries initNodeState sender appendEntries
        AppendEntriesResponseRPC appendEntriesResp ->
          handleAppendEntriesResponse initNodeState sender appendEntriesResp
        RequestVoteRPC requestVote ->
          handleRequestVote initNodeState sender requestVote
        RequestVoteResponseRPC requestVoteResp ->
          handleRequestVoteResponse initNodeState sender requestVoteResp

    handleClientRequest :: ClientRequest v -> TransitionM sm v (ResultState ns sm v)
    handleClientRequest (ClientRequest cid cr) =
      case cr of
        ClientReadReq crr -> handleClientReadRequest initNodeState cid crr
        ClientWriteReq cwr -> handleClientWriteRequest initNodeState cid cwr
        -- All nodes handle this request the same
        ClientMetricsReq _ -> do
          respondClientMetrics cid
          pure $ case initNodeState of
            NodeFollowerState fs -> followerResultState Noop fs
            NodeCandidateState cs -> candidateResultState Noop cs
            NodeLeaderState ls -> leaderResultState Noop ls
