{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}

module Raft.RPC where

import Protolude

import Data.Serialize

import Raft.Log
import Raft.Types

-- | Interface for nodes to send messages to one
-- another. E.g. Control.Concurrent.Chan, Network.Socket, etc.
class RaftSendRPC m v where
  sendRPC :: NodeId -> RPCMessage v -> m ()

-- | Interface for nodes to receive messages from one
-- another
class Show (RaftRecvRPCError m v) => RaftRecvRPC m v where
  type RaftRecvRPCError m v
  receiveRPC :: m (Either (RaftRecvRPCError m v) (RPCMessage v))

-- | Representation of a message sent between nodes
data RPCMessage v = RPCMessage
  { sender :: NodeId
  , rpc :: RPC v
  } deriving (Show, Generic, Serialize)

data RPC v
  = AppendEntriesRPC (AppendEntries v)
  | AppendEntriesResponseRPC AppendEntriesResponse
  | RequestVoteRPC RequestVote
  | RequestVoteResponseRPC RequestVoteResponse
  deriving (Show, Generic, Serialize)

class RPCType a v where
  toRPC :: a -> RPC v

instance RPCType (AppendEntries v) v where
  toRPC = AppendEntriesRPC

instance RPCType AppendEntriesResponse v where
  toRPC = AppendEntriesResponseRPC

instance RPCType RequestVote v where
  toRPC = RequestVoteRPC

instance RPCType RequestVoteResponse v where
  toRPC = RequestVoteResponseRPC

rpcTerm :: RPC v -> Term
rpcTerm = \case
  AppendEntriesRPC ae -> aeTerm ae
  AppendEntriesResponseRPC aer -> aerTerm aer
  RequestVoteRPC rv -> rvTerm rv
  RequestVoteResponseRPC rvr -> rvrTerm rvr

data NoEntriesSpec
  = FromInconsistency
  | FromHeartbeat
  deriving (Show)

data EntriesSpec v
  = FromIndex Index
  | FromNewLeader (Entry v)
  | FromClientReq (Entry v)
  | NoEntries NoEntriesSpec
  deriving (Show)

-- | The data used to construct an AppendEntries value, snapshotted from the
-- node state at the time the AppendEntries val should be created.
data AppendEntriesData v = AppendEntriesData
  { aedTerm :: Term
  , aedLeaderCommit :: Index
  , aedEntriesSpec :: EntriesSpec v
  } deriving (Show)

-- | Representation of a message sent from a leader to its peers
data AppendEntries v = AppendEntries
  { aeTerm :: Term
    -- ^ Leader's term
  , aeLeaderId :: LeaderId
    -- ^ Leader's identifier so that followers can redirect clients
  , aePrevLogIndex :: Index
    -- ^ Index of log entry immediately preceding new ones
  , aePrevLogTerm :: Term
    -- ^ Term of aePrevLogIndex entry
  , aeEntries :: Entries v
    -- ^ Log entries to store (empty for heartbeat)
  , aeLeaderCommit :: Index
    -- ^ Leader's commit index
  } deriving (Show, Generic, Serialize)

-- | Representation of the response from a follower to an AppendEntries message
data AppendEntriesResponse = AppendEntriesResponse
  { aerTerm :: Term
    -- ^ current term for leader to update itself
  , aerSuccess :: Bool
    -- ^ true if follower contained entry matching aePrevLogIndex and aePrevLogTerm
  } deriving (Show, Generic, Serialize)

-- | Representation of the message sent by candidates to their peers to request
-- their vote
data RequestVote = RequestVote
  { rvTerm :: Term
    -- ^ candidates term
  , rvCandidateId :: NodeId
    -- ^ candidate requesting vote
  , rvLastLogIndex :: Index
    -- ^ index of candidate's last log entry
  , rvLastLogTerm :: Term
    -- ^ term of candidate's last log entry
  } deriving (Show, Generic, Serialize)

-- | Representation of a response to a RequestVote message
data RequestVoteResponse = RequestVoteResponse
  { rvrTerm :: Term
    -- ^ current term for candidate to update itself
  , rvrVoteGranted :: Bool
    -- ^ true means candidate recieved vote
  } deriving (Show, Generic, Serialize)
