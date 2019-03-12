{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}

module Raft.Leader (
    handleAppendEntries
  , handleAppendEntriesResponse
  , handleRequestVote
  , handleRequestVoteResponse
  , handleTimeout
  , handleClientReadRequest
  , handleClientWriteRequest
) where

import Protolude

import qualified Data.Map as Map
import Data.Serialize (Serialize)
import Data.Sequence (Seq(Empty))
import qualified Data.Set as Set
import qualified Data.Sequence as Seq

import Raft.Config (raftConfigNodeIds)
import Raft.NodeState
import Raft.RPC
import Raft.Action
import Raft.Client
import Raft.Event
import Raft.Persistent
import Raft.Log (Entry(..), EntryIssuer(..), EntryValue(..))
import Raft.Transition
import Raft.Types

--------------------------------------------------------------------------------
-- Leader
--------------------------------------------------------------------------------

-- | Leaders should not respond to 'AppendEntries' messages.
handleAppendEntries :: RPCHandler 'Leader sm (AppendEntries v) v
handleAppendEntries (NodeLeaderState ls)_ _  =
  pure (leaderResultState Noop ls)

handleAppendEntriesResponse :: forall sm v. RPCHandler 'Leader sm AppendEntriesResponse v
handleAppendEntriesResponse ns@(NodeLeaderState ls) sender appendEntriesResp
  -- If AppendEntries fails (aerSuccess == False) because of log inconsistency,
  -- decrement nextIndex and retry
  | not (aerSuccess appendEntriesResp) = do
      let newNextIndices = Map.adjust decrIndexWithDefault0 sender (lsNextIndex ls)
          newLeaderState = ls { lsNextIndex = newNextIndices }
          Just newNextIndex = Map.lookup sender newNextIndices
      aeData <- mkAppendEntriesData newLeaderState (FromIndex newNextIndex)
      send sender (SendAppendEntriesRPC aeData)
      pure (leaderResultState Noop newLeaderState)
  | otherwise = do
      case aerReadRequest appendEntriesResp of
        Nothing -> leaderResultState Noop <$> do
          let lastLogEntryIdx = lastLogEntryIndex (lsLastLogEntry ls)
              newNextIndices = Map.insert sender (lastLogEntryIdx + 1) (lsNextIndex ls)
              newMatchIndices = Map.insert sender lastLogEntryIdx (lsMatchIndex ls)
              lsUpdatedIndices = ls { lsNextIndex = newNextIndices, lsMatchIndex = newMatchIndices }
          -- Increment leader commit index if now a majority of followers have
          -- replicated an entry at a given term.
          lsUpdatedCommitIdx <- incrCommitIndex lsUpdatedIndices
          when (lsCommitIndex lsUpdatedCommitIdx > lsCommitIndex lsUpdatedIndices) $
            updateClientReqCacheFromIdx (lsCommitIndex lsUpdatedIndices)
          pure lsUpdatedCommitIdx
        Just n -> handleReadReq n ls
  where
    handleReadReq :: Int -> LeaderState v -> TransitionM sm v (ResultState 'Leader v)
    handleReadReq n leaderState = do
      networkSize <- Set.size <$> asks (raftConfigNodeIds . nodeConfig)
      let initReadReqs = lsReadRequest leaderState
          (mCreqData, newReadReqs) =
            case Map.lookup n initReadReqs of
              -- In this case, the client who issued the read request has been
              -- responded to already (i.e. this is an extraneous confirmation)
              Nothing -> (Nothing, initReadReqs)
              -- Otherwise, the client has not been responded to, because the
              -- leader has not accumulated enough confirmations of its state
              -- as network leader.
              Just (creqData, m)
                | isMajority (succ m) networkSize -> (Just creqData, Map.delete n initReadReqs)
                | otherwise -> (Nothing, Map.adjust (second succ) n initReadReqs)
      case mCreqData of
        Nothing ->
          pure $ leaderResultState Noop leaderState
            { lsReadRequest = newReadReqs
            }
        Just (ClientReadReqData cid res) -> do
          respondClientRead cid res
          pure $ leaderResultState Noop leaderState
            { lsReadReqsHandled = succ (lsReadReqsHandled leaderState)
            , lsReadRequest = newReadReqs
            }

-- | Leaders should not respond to 'RequestVote' messages.
handleRequestVote :: RPCHandler 'Leader sm RequestVote v
handleRequestVote (NodeLeaderState ls) _ _ =
  pure (leaderResultState Noop ls)

-- | Leaders should not respond to 'RequestVoteResponse' messages.
handleRequestVoteResponse :: RPCHandler 'Leader sm RequestVoteResponse v
handleRequestVoteResponse (NodeLeaderState ls) _ _ =
  pure (leaderResultState Noop ls)

handleTimeout :: Show v => TimeoutHandler 'Leader sm v
handleTimeout (NodeLeaderState ls) timeout =
  case timeout of
    -- Leader does not handle election timeouts
    ElectionTimeout -> pure (leaderResultState Noop ls)
    -- On a heartbeat timeout, broadcast append entries RPC to all peers
    HeartbeatTimeout -> do
      aeData <- mkAppendEntriesData ls (NoEntries FromHeartbeat)
      broadcast (SendAppendEntriesRPC aeData)
      pure (leaderResultState SendHeartbeat ls)

handleClientReadRequest :: (Show v, Serialize v) => ClientReqHandler 'Leader ClientReadReq sm v
handleClientReadRequest (NodeLeaderState ls@LeaderState{..}) cid crr = do
  heartbeat <- mkAppendEntriesData ls (NoEntries (FromClientReadReq lsReadReqsHandled))
  broadcast (SendAppendEntriesRPC heartbeat)
  let clientReqData = ClientReadReqData cid crr
  pure $ leaderResultState HandleClientReq ls {
    lsReadRequest =
      Map.insert lsReadReqsHandled (clientReqData, 1) lsReadRequest
  }

handleClientWriteRequest :: (Show v, Serialize v) => ClientReqHandler 'Leader (ClientWriteReq v) sm v
handleClientWriteRequest (NodeLeaderState ls@LeaderState{..}) cid (ClientCmdReq serial v) =
  leaderResultState HandleClientReq <$>
    case Map.lookup cid lsClientReqCache of
      -- This is important case #1
      Nothing -> handleNewEntry serial v
      Just (currSerial, mResp)
        | serial < currSerial -> do
            let debugMsg s1 s2 = "Ignoring serial number " <> s1 <> ", current serial is " <> s2
            logDebug $ debugMsg (show serial) (show currSerial)
            pure ls
        | currSerial == serial -> do
            case mResp of
              Nothing -> logDebug $ "Serial " <> show currSerial <> " already exists. Ignoring repeat request."
              Just idx -> respondClientWrite cid idx serial
            pure ls
        -- This is important case #2, where serial > currSerial
        | otherwise -> handleNewEntry serial v
  where
    handleNewEntry serial cmd = do
      let lsClientReqCache' = Map.insert cid (serial, Nothing) lsClientReqCache
      newLogEntry <- mkNewLogEntry cmd serial
      appendLogEntries (Empty Seq.|> newLogEntry)
      aeData <- mkAppendEntriesData ls (FromClientWriteReq newLogEntry)
      broadcast (SendAppendEntriesRPC aeData)
      pure ls { lsClientReqCache = lsClientReqCache' }

    mkNewLogEntry v sn = do
      currentTerm <- currentTerm <$> get
      let lastLogEntryIdx = lastLogEntryIndex lsLastLogEntry
      pure $ Entry
        { entryIndex = succ lastLogEntryIdx
        , entryTerm = currentTerm
        , entryValue = EntryValue v
        , entryIssuer = ClientIssuer cid sn
        , entryPrevHash = hashLastLogEntry lsLastLogEntry
        }

--------------------------------------------------------------------------------

-- | If there exists an N such that N > commitIndex, a majority of
-- matchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex = N
incrCommitIndex :: Show v => LeaderState v -> TransitionM sm v (LeaderState v)
incrCommitIndex leaderState@LeaderState{..} = do
    logDebug "Checking if commit index should be incremented..."
    let lastEntryTerm = lastLogEntryTerm lsLastLogEntry
    currentTerm <- currentTerm <$> get
    if majorityGreaterThanN && (lastEntryTerm == currentTerm)
      then do
        logDebug $ "Incrementing commit index to: " <> show n
        incrCommitIndex leaderState { lsCommitIndex = n }
      else do
        logDebug "Not incrementing commit index."
        pure leaderState
  where
    n = lsCommitIndex + 1

    -- Note: The majority should include the leader, which is not accounted for
    -- in the lsMatchIndex map, thus, we should add one to both the number of
    -- followers that have a match index >= n, as well as the total size of
    -- 'lsMatchIndex'. E.G. Imagine a network of 2 followers and 1 leader; in
    -- the case where one of the followers has a match index >= n, technically a
    -- majority has replicated the instance if you include the leader. However,
    -- without adding one to each parameter of 'isMajority' (not accounting for
    -- the leader) would result in False, because 1 out of 2 followers is not a
    -- majority.
    majorityGreaterThanN =
      isMajority (Map.size (Map.filter (>= n) lsMatchIndex) + 1)
                 (Map.size lsMatchIndex + 1)

isMajority :: Int -> Int -> Bool
isMajority n m = n > m `div` 2

-- | Construct an AppendEntriesRPC given log entries and a leader state.
mkAppendEntriesData
  :: Show v
  => LeaderState v
  -> AppendEntriesSpec v
  -> TransitionM sm v (AppendEntriesData v)
mkAppendEntriesData ls entriesSpec = do
  currentTerm <- gets currentTerm
  pure AppendEntriesData
    { aedTerm = currentTerm
    , aedLeaderCommit = lsCommitIndex ls
    , aedEntriesSpec = entriesSpec
    }
