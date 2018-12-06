{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
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
  , handleClientRequest
) where

import Protolude

import qualified Data.Map as Map
import Data.Serialize (Serialize)
import Data.Sequence (Seq(Empty))
import qualified Data.Set as Set
import qualified Data.Sequence as Seq

import Raft.Config (configNodeIds)
import Raft.NodeState
import Raft.RPC
import Raft.Action
import Raft.Client
import Raft.Event
import Raft.Persistent
import Raft.Log (Entry(..), EntryIssuer(..), EntryValue(..))
import Raft.Monad
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
      let lastLogEntryIdx = lastLogEntryIndex (lsLastLogEntry ls)
          newNextIndices = Map.insert sender (lastLogEntryIdx + 1) (lsNextIndex ls)
          newMatchIndices = Map.insert sender lastLogEntryIdx (lsMatchIndex ls)
          newLeaderState = ls { lsNextIndex = newNextIndices, lsMatchIndex = newMatchIndices }
      -- Increment leader commit index if now a majority of followers have
      -- replicated an entry at a given term.
      newestLeaderState <- incrCommitIndex newLeaderState
      when (lsCommitIndex newestLeaderState > lsCommitIndex newLeaderState) $ do
        let lastLogEntry = lsLastLogEntry newestLeaderState
            entryIdx = lastLogEntryIndex lastLogEntry
            entryIssuer = lastLogEntryIssuer lastLogEntry
        case entryIssuer of
          Nothing -> panic "No last log entry issuer"
          Just (LeaderIssuer _) -> pure ()
          Just (ClientIssuer cid) -> tellActions [RespondToClient cid (ClientWriteResponse (ClientWriteResp entryIdx))]

      case aerReadRequest appendEntriesResp of
        Nothing -> pure (leaderResultState Noop newestLeaderState)
        Just n -> handleReadReq n newestLeaderState
  where
    handleReadReq :: Int -> LeaderState v -> TransitionM sm v (ResultState 'Leader v)
    handleReadReq n leaderState = do
      networkSize <- Set.size <$> asks (configNodeIds . nodeConfig)
      let initReadReqs = lsReadRequest leaderState
          (mclientId, newReadReqs) =
            case Map.lookup n initReadReqs of
              Nothing -> (Nothing, initReadReqs)
              Just (cid,m)
                | isMajority (succ m) networkSize -> (Just cid, Map.delete n initReadReqs)
                | otherwise -> (Nothing, Map.adjust (second succ) n initReadReqs)
      case mclientId of
        Nothing ->
          pure $ leaderResultState Noop leaderState
            { lsReadRequest = newReadReqs
            }
        Just cid -> do
          respondClientRead cid
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

-- | The leader handles all client requests, responding with the current state
-- machine on a client read, and appending an entry to the log on a valid client
-- write.
handleClientRequest :: (Show v, Serialize v) => ClientReqHandler 'Leader sm v
handleClientRequest (NodeLeaderState ls@LeaderState{..}) (ClientRequest cid cr) = do
    case cr of
      ClientReadReq -> do
        heartbeat <- mkAppendEntriesData ls (NoEntries (FromClientReadReq lsReadReqsHandled))
        broadcast (SendAppendEntriesRPC heartbeat)
        let newLeaderState =
              ls { lsReadRequest = Map.insert lsReadReqsHandled (cid, 1) lsReadRequest
                 }
        pure (leaderResultState Noop newLeaderState)
      ClientWriteReq v -> do
        newLogEntry <- mkNewLogEntry v
        appendLogEntries (Empty Seq.|> newLogEntry)
        aeData <- mkAppendEntriesData ls (FromClientWriteReq newLogEntry)
        broadcast (SendAppendEntriesRPC aeData)
        pure (leaderResultState Noop ls)
  where
    mkNewLogEntry v = do
      currentTerm <- currentTerm <$> get
      let lastLogEntryIdx = lastLogEntryIndex lsLastLogEntry
      pure $ Entry
        { entryIndex = succ lastLogEntryIdx
        , entryTerm = currentTerm
        , entryValue = EntryValue v
        , entryIssuer = ClientIssuer cid
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

    -- Note, the majority in this case includes the leader itself, thus when
    -- calculating the number of nodes that need a match index > N, we only need
    -- to divide the size of the map by 2 instead of also adding one.
    majorityGreaterThanN =
      isMajority (Map.size (Map.filter (>= n) lsMatchIndex) + 1)
                 (Map.size lsMatchIndex)

isMajority :: Int -> Int -> Bool
isMajority n m = n >= m `div` 2 + 1

-- | Construct an AppendEntriesRPC given log entries and a leader state.
mkAppendEntriesData
  :: Show v
  => LeaderState v
  -> EntriesSpec v
  -> TransitionM sm v (AppendEntriesData v)
mkAppendEntriesData ls entriesSpec = do
  currentTerm <- gets currentTerm
  pure AppendEntriesData
    { aedTerm = currentTerm
    , aedLeaderCommit = lsCommitIndex ls
    , aedEntriesSpec = entriesSpec
    }
