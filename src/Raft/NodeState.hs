{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE StrictData #-}

module Raft.NodeState where

import Protolude

import qualified Data.Serialize as S
import Data.Sequence (Seq(..))

import Raft.Client
import Raft.Log
import Raft.Client (SerialNum)
import Raft.Types

data Mode
  = Follower
  | Candidate
  | Leader
  deriving (Show)

-- | All valid state transitions of a Raft node
data Transition (init :: Mode) (res :: Mode) where
  StartElection            :: Transition 'Follower 'Candidate
  HigherTermFoundFollower  :: Transition 'Follower 'Follower

  RestartElection          :: Transition 'Candidate 'Candidate
  DiscoverLeader           :: Transition 'Candidate 'Follower
  HigherTermFoundCandidate :: Transition 'Candidate 'Follower
  BecomeLeader             :: Transition 'Candidate 'Leader

  HandleClientReq          :: Transition 'Leader 'Leader
  SendHeartbeat            :: Transition 'Leader 'Leader
  DiscoverNewLeader        :: Transition 'Leader 'Follower
  HigherTermFoundLeader    :: Transition 'Leader 'Follower

  Noop :: Transition init init
deriving instance Show (Transition init res)

-- | Existential type hiding the result type of a transition
data ResultState init v where
  ResultState
    :: Show v
    => Transition init res
    -> NodeState res v
    -> ResultState init v

deriving instance Show v => Show (ResultState init v)

followerResultState
  :: Show v
  => Transition init 'Follower
  -> FollowerState v
  -> ResultState init v
followerResultState transition fstate =
  ResultState transition (NodeFollowerState fstate)

candidateResultState
  :: Show v
  => Transition init 'Candidate
  -> CandidateState v
  -> ResultState init v
candidateResultState transition cstate =
  ResultState transition (NodeCandidateState cstate)

leaderResultState
  :: Show v
  => Transition init 'Leader
  -> LeaderState v
  -> ResultState init v
leaderResultState transition lstate =
  ResultState transition (NodeLeaderState lstate)

-- | Existential type hiding the internal node state
data RaftNodeState v where
  RaftNodeState :: { unRaftNodeState :: NodeState s v } -> RaftNodeState v

deriving instance Show v => Show (RaftNodeState v)

nodeMode :: RaftNodeState v -> Mode
nodeMode (RaftNodeState rns) =
  case rns of
    NodeFollowerState _ -> Follower
    NodeCandidateState _ -> Candidate
    NodeLeaderState _ -> Leader

-- | A node in Raft begins as a follower
initRaftNodeState :: RaftNodeState v
initRaftNodeState =
  RaftNodeState $
    NodeFollowerState FollowerState
      { fsCommitIndex = index0
      , fsLastApplied = index0
      , fsCurrentLeader = NoLeader
      , fsLastLogEntry = NoLogEntries
      , fsTermAtAEPrevIndex = Nothing
      , fsClientReqCache = mempty
      }

-- | The volatile state of a Raft Node
data NodeState (a :: Mode) v where
  NodeFollowerState :: FollowerState v -> NodeState 'Follower v
  NodeCandidateState :: CandidateState v -> NodeState 'Candidate v
  NodeLeaderState :: LeaderState v -> NodeState 'Leader v

deriving instance Show v => Show (NodeState s v)

data LastLogEntry v
  = LastLogEntry (Entry v)
  | NoLogEntries
  deriving (Show)

hashLastLogEntry :: S.Serialize v => LastLogEntry v -> EntryHash
hashLastLogEntry = \case
  LastLogEntry e -> hashEntry e
  NoLogEntries -> genesisHash

lastLogEntryIndex :: LastLogEntry v -> Index
lastLogEntryIndex = \case
  LastLogEntry e -> entryIndex e
  NoLogEntries -> index0

lastLogEntryTerm :: LastLogEntry v -> Term
lastLogEntryTerm = \case
  LastLogEntry e -> entryTerm e
  NoLogEntries -> term0

lastLogEntryIndexAndTerm :: LastLogEntry v -> (Index, Term)
lastLogEntryIndexAndTerm lle = (lastLogEntryIndex lle, lastLogEntryTerm lle)

lastLogEntryIssuer :: LastLogEntry v -> Maybe EntryIssuer
lastLogEntryIssuer = \case
  LastLogEntry e -> Just (entryIssuer e)
  NoLogEntries -> Nothing

data FollowerState v = FollowerState
  { fsCurrentLeader :: CurrentLeader
    -- ^ Id of the current leader
  , fsCommitIndex :: Index
    -- ^ Index of highest log entry known to be committed
  , fsLastApplied :: Index
    -- ^ Index of highest log entry applied to state machine
  , fsLastLogEntry :: LastLogEntry v
    -- ^ Index and term of the last log entry in the node's log
  , fsTermAtAEPrevIndex :: Maybe Term
    -- ^ The term of the log entry specified in and AppendEntriesRPC
  , fsClientReqCache :: ClientWriteReqCache
    -- ^ The client write request cache, growing linearly with the number of
    -- clients
  } deriving (Show)

data CandidateState v = CandidateState
  { csCommitIndex :: Index
    -- ^ Index of highest log entry known to be committed
  , csLastApplied :: Index
    -- ^ Index of highest log entry applied to state machine
  , csVotes :: NodeIds
    -- ^ Votes from other nodes in the raft network
  , csLastLogEntry :: LastLogEntry v
    -- ^ Index and term of the last log entry in the node's log
  , csClientReqCache :: ClientWriteReqCache
    -- ^ The client write request cache, growing linearly with the number of
    -- clients
  } deriving (Show)

data ClientReadReqData = ClientReadReqData
  { crrdClientId :: ClientId
  , crrdReadReq  :: ClientReadReq
  } deriving (Show)

-- | The type mapping the number of the read request serviced to the id of the
-- client that issued it and the number of success responses from followers
-- confirming the leadership of the current leader
type ClientReadReqs = Map Int (ClientReadReqData, Int)

-- | The type mapping client ids to the serial number of their latest write
-- requests and the index of the entry if it has been replicated.
type ClientWriteReqCache = Map ClientId (SerialNum, Maybe Index)

data LeaderState v = LeaderState
  { lsCommitIndex :: Index
    -- ^ Index of highest log entry known to be committed
  , lsLastApplied :: Index
    -- ^ Index of highest log entry applied to state machine
  , lsNextIndex :: Map NodeId Index
    -- ^ For each server, index of the next log entry to send to that server
  , lsMatchIndex :: Map NodeId Index
    -- ^ For each server, index of highest log entry known to be replicated on server
  , lsLastLogEntry :: LastLogEntry v
    -- ^ Index, term, and client id of the last log entry in the node's log.
    -- The only time `Maybe ClientId` will be Nothing is at the initial term.
  , lsReadReqsHandled :: Int
    -- ^ Number of read requests handled this term
  , lsReadRequest :: ClientReadReqs
    -- ^ The number of successful responses received regarding a specific read
    -- request heartbeat.
  , lsClientReqCache :: ClientWriteReqCache
    -- ^ The cache of client write requests received by the leader
  } deriving (Show)

--------------------------------------------------------------------------------
-- Helpers
--------------------------------------------------------------------------------

-- | Update the last log entry in the node's log
setLastLogEntry :: NodeState s v -> Entries v -> NodeState s v
setLastLogEntry nodeState entries =
  case entries of
    Empty -> nodeState
    _ :|> e -> do
      let lastLogEntry = LastLogEntry e
      case nodeState of
        NodeFollowerState fs ->
          NodeFollowerState fs { fsLastLogEntry = lastLogEntry }
        NodeCandidateState cs ->
          NodeCandidateState cs { csLastLogEntry = lastLogEntry }
        NodeLeaderState ls ->
          NodeLeaderState ls { lsLastLogEntry = lastLogEntry }

-- | Get the last applied index and the commit index of the last log entry in
-- the node's log
getLastLogEntry :: NodeState ns v -> LastLogEntry v
getLastLogEntry nodeState =
  case nodeState of
    NodeFollowerState fs -> fsLastLogEntry fs
    NodeCandidateState cs -> csLastLogEntry cs
    NodeLeaderState ls -> lsLastLogEntry ls

-- | Get the index of highest log entry applied to state machine and the index
-- of highest log entry known to be committed
getLastAppliedAndCommitIndex :: NodeState ns v -> (Index, Index)
getLastAppliedAndCommitIndex nodeState =
  case nodeState of
    NodeFollowerState fs -> (fsLastApplied fs, fsCommitIndex fs)
    NodeCandidateState cs -> (csLastApplied cs, csCommitIndex cs)
    NodeLeaderState ls -> (lsLastApplied ls, lsCommitIndex ls)

-- | Check if node is in a follower state
isFollower :: NodeState s v -> Bool
isFollower nodeState =
  case nodeState of
    NodeFollowerState _ -> True
    _ -> False

-- | Check if node is in a candidate state
isCandidate :: NodeState s v -> Bool
isCandidate nodeState =
  case nodeState of
    NodeCandidateState _ -> True
    _ -> False

-- | Check if node is in a leader state
isLeader :: NodeState s v -> Bool
isLeader nodeState =
  case nodeState of
    NodeLeaderState _ -> True
    _ -> False
