{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE MonoLocalBinds #-}

module Raft.Follower (
    handleAppendEntries
  , handleAppendEntriesResponse
  , handleRequestVote
  , handleRequestVoteResponse
  , handleTimeout
  , handleClientReadRequest
  , handleClientWriteRequest
) where

import Protolude

import Data.Sequence (Seq(..))

import Raft.Action
import Raft.NodeState
import Raft.RPC
import Raft.Client
import Raft.Event
import Raft.Persistent
import Raft.Log
import Raft.Transition
import Raft.Types

--------------------------------------------------------------------------------
-- Follower
--------------------------------------------------------------------------------

-- | Handle AppendEntries RPC message from Leader
-- Sections 5.2 and 5.3 of Raft Paper & Figure 2: Receiver Implementation
--
-- Note: see 'PersistentState' datatype for discussion about not keeping the
-- entire log in memory.
handleAppendEntries :: forall v sm. Show v => RPCHandler 'Follower sm (AppendEntries v) v
handleAppendEntries ns@(NodeFollowerState fs) sender ae@AppendEntries{..} = do
  PersistentState{..} <- get

  let status = shouldApplyAppendEntries currentTerm fs ae
  newFollowerState <-
    case status of
      AERSuccess _ -> do
        -- 3. If an existing entry conflicts with a new one (same index
        -- but different terms), delete the existing entry and all that
        -- follow it.
        --   &
        -- 4. Append any new entries not already in the log
        appendLogEntries aeEntries
        resetElectionTimeout
        -- | 5. If leaderCommit > commitIndex, set commitIndex =
        -- min(leaderCommit, index of last new entry)
        pure $
          if aeLeaderCommit > fsCommitIndex fs
            then updateLeader (updateCommitIndex fs)
            else updateLeader fs
      _ -> pure fs

  send (unLeaderId aeLeaderId) $
    SendAppendEntriesResponseRPC
      AppendEntriesResponse
        { aerTerm = currentTerm
        , aerStatus = status
        , aerReadRequest = aeReadRequest
        }
  pure (followerResultState Noop newFollowerState)
  where
    updateCommitIndex :: FollowerState v -> FollowerState v
    updateCommitIndex followerState =
      case aeEntries of
        Empty -> followerState
        _ :|> e ->
          let newCommitIndex = min aeLeaderCommit (entryIndex e)
          in followerState { fsCommitIndex = newCommitIndex }

    updateLeader :: FollowerState v -> FollowerState v
    updateLeader followerState = followerState { fsCurrentLeader = CurrentLeader (LeaderId sender) }

-- | Decide if entries given can be applied
shouldApplyAppendEntries :: Term -> FollowerState v -> AppendEntries v -> AppendEntriesResponseStatus
shouldApplyAppendEntries currentTerm fs AppendEntries{..} =
  if aeTerm < currentTerm
    -- 1. Reply false if term < currentTerm
    then AERStaleTerm
    else
      case fsTermAtAEPrevIndex fs of
        Nothing
          -- there are no previous entries
          | aePrevLogIndex == index0 -> AERSuccess {aerLatestIndex = Index (fromIntegral $ length aeEntries)}
          -- the follower doesn't have the previous index given in the AppendEntriesRPC
          -- the entries we are receiving are beyond the end of our log
          -- so we respond with the index of the followers last entry + 1
          | otherwise -> case fsLastLogEntry fs of
              NoLogEntries -> AERInconsistent {aerNextIndex = incrIndex index0}
              LastLogEntry entry -> AERInconsistent { aerNextIndex = incrIndex $ entryIndex entry}
        Just entryAtAEPrevLogIndexTerm ->
          -- 2. Reply false if log doesn't contain an entry at
          -- prevLogIndex whose term matches prevLogTerm.
          if entryAtAEPrevLogIndexTerm /= aePrevLogTerm
            -- In this case, our log contains an entry that conflicts with the leaders entries
            -- TODO could try a binary search but ( as the paper says ) in practice
            -- we doubt this optimisation is necessary, since failures happen infrequently
            -- and it is unlikely there will be many inconsistent entries
            then AERInconsistent
              { aerNextIndex = fsCommitIndex fs }
            else AERSuccess { aerLatestIndex = aePrevLogIndex + Index (fromIntegral $ length aeEntries)}

-- | Followers should not respond to 'AppendEntriesResponse' messages.
handleAppendEntriesResponse :: RPCHandler 'Follower sm AppendEntriesResponse v
handleAppendEntriesResponse (NodeFollowerState fs) _ _ =
  pure (followerResultState Noop fs)

handleRequestVote :: RPCHandler 'Follower sm RequestVote v
handleRequestVote ns@(NodeFollowerState fs) sender RequestVote{..} = do
    PersistentState{..} <- get
    let voteGranted = giveVote currentTerm votedFor
    when voteGranted $ do
      modify $ \pstate ->
        pstate { votedFor = Just sender }
      resetElectionTimeout
    send sender $
      SendRequestVoteResponseRPC $
        RequestVoteResponse
          { rvrTerm = currentTerm
          , rvrVoteGranted = voteGranted
          }
    pure $ followerResultState Noop fs
  where
    giveVote term mVotedFor =
      and [ term <= rvTerm
          , validCandidateId mVotedFor
          , validCandidateLog
          ]

    validCandidateId Nothing = True
    validCandidateId (Just cid) = cid == rvCandidateId

    -- Check if the requesting candidate's log is more up to date
    -- Section 5.4.1 in Raft Paper
    validCandidateLog =
      let (lastEntryIdx, lastEntryTerm) = lastLogEntryIndexAndTerm (fsLastLogEntry fs)
       in (rvLastLogTerm > lastEntryTerm)
       || (rvLastLogTerm == lastEntryTerm && rvLastLogIndex >= lastEntryIdx)

-- | Followers should not respond to 'RequestVoteResponse' messages.
handleRequestVoteResponse :: RPCHandler 'Follower sm RequestVoteResponse v
handleRequestVoteResponse (NodeFollowerState fs) _ _  =
  pure (followerResultState Noop fs)

-- | Follower converts to Candidate if handling ElectionTimeout
handleTimeout :: TimeoutHandler 'Follower sm v
handleTimeout ns@(NodeFollowerState fs) timeout =
  case timeout of
    ElectionTimeout -> do
      logDebug "Follower times out. Starts election. Becomes candidate"
      candidateResultState StartElection <$>
        startElection (fsCommitIndex fs) (fsLastApplied fs) (fsLastLogEntry fs) (fsClientReqCache fs)
    -- Follower should ignore heartbeat timeout events
    HeartbeatTimeout -> pure (followerResultState Noop fs)


-- | When a client handles a client request, it redirects the client to the
-- current leader by responding with the current leader id, if it knows of one.
handleClientReadRequest :: ClientReqHandler 'Follower ClientReadReq sm v
handleClientReadRequest = handleClientRequest

-- | When a client handles a client request, it redirects the client to the
-- current leader by responding with the current leader id, if it knows of one.
handleClientWriteRequest :: ClientReqHandler 'Follower (ClientWriteReq v) sm v
handleClientWriteRequest = handleClientRequest

-- | When a client handles a client request, it redirects the client to the
-- current leader by responding with the current leader id, if it knows of one.
handleClientRequest :: ClientReqHandler 'Follower cr sm v
handleClientRequest (NodeFollowerState fs) clientId _ = do
  redirectClientToLeader clientId (fsCurrentLeader fs)
  pure (followerResultState Noop fs)
