{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE GADTs #-}

module TestUtils where

import Protolude
import qualified Data.Set as Set
import qualified Data.Map.Merge.Lazy as Merge

import Raft
import Raft.Config

isRaftLeader :: RaftNodeState v -> Bool
isRaftLeader (RaftNodeState rns) = isLeader rns

isRaftCandidate :: RaftNodeState v -> Bool
isRaftCandidate (RaftNodeState rns) = isCandidate rns

isRaftFollower :: RaftNodeState v -> Bool
isRaftFollower (RaftNodeState rns) = isFollower rns

checkCurrentLeader :: RaftNodeState v -> CurrentLeader
checkCurrentLeader (RaftNodeState (NodeFollowerState FollowerState{..})) = fsCurrentLeader
checkCurrentLeader (RaftNodeState (NodeCandidateState _)) = NoLeader
checkCurrentLeader (RaftNodeState (NodeLeaderState _)) = NoLeader

getLastAppliedLog :: RaftNodeState v -> Index
getLastAppliedLog (RaftNodeState (NodeFollowerState FollowerState{..})) = fsLastApplied
getLastAppliedLog (RaftNodeState (NodeCandidateState CandidateState{..})) = csLastApplied
getLastAppliedLog (RaftNodeState (NodeLeaderState LeaderState{..})) = lsLastApplied

getCommittedLogIndex :: RaftNodeState v -> Index
getCommittedLogIndex (RaftNodeState (NodeFollowerState FollowerState{..})) = fsCommitIndex
getCommittedLogIndex (RaftNodeState (NodeCandidateState CandidateState{..})) = csCommitIndex
getCommittedLogIndex (RaftNodeState (NodeLeaderState LeaderState{..})) = lsCommitIndex

node0, node1, node2 :: NodeId
node0 = "node0"
node1 = "node1"
node2 = "node2"

client0 :: ClientId
client0 = ClientId "client0"

nodeIds :: NodeIds
nodeIds = Set.fromList [node0, node1, node2]

testConfigs :: [RaftNodeConfig]
testConfigs = [testConfig0, testConfig1, testConfig2]

msToMicroS :: Num n => n -> n
msToMicroS = (1000 *)

pairMsToMicroS :: Num n => (n, n) -> (n, n)
pairMsToMicroS = bimap msToMicroS msToMicroS

testConfig0, testConfig1, testConfig2 :: RaftNodeConfig
testConfig0 = RaftNodeConfig
  { raftConfigNodeId = node0
  , raftConfigNodeIds = nodeIds
  , raftConfigElectionTimeout = pairMsToMicroS (150, 300)
  , raftConfigHeartbeatTimeout = msToMicroS 50
  , raftConfigStorageState = New
  }
testConfig1 = RaftNodeConfig
  { raftConfigNodeId = node1
  , raftConfigNodeIds = nodeIds
  , raftConfigElectionTimeout = pairMsToMicroS (150, 300)
  , raftConfigHeartbeatTimeout = msToMicroS 50
  , raftConfigStorageState = New
  }
testConfig2 = RaftNodeConfig
  { raftConfigNodeId = node2
  , raftConfigNodeIds = nodeIds
  , raftConfigElectionTimeout = pairMsToMicroS (150, 300)
  , raftConfigHeartbeatTimeout = msToMicroS 50
  , raftConfigStorageState = New
  }

-- | Zip maps using function. Throws away items left and right
zipMapWith :: Ord k => (a -> b -> c) -> Map k a -> Map k b -> Map k c
zipMapWith f = Merge.merge Merge.dropMissing Merge.dropMissing (Merge.zipWithMatched (const f))

-- | Perform an inner join on maps (hence throws away items left and right)
combine :: Ord a => Map a b -> Map a c -> Map a (b, c)
combine = zipMapWith (,)

printIfNode :: (Show nId, Eq nId) => nId -> nId -> [Char] -> IO ()
printIfNode nId nId' msg =
  when (nId == nId') $
    print $ show nId ++ " " ++ msg

printIfNodes :: (Show nId, Eq nId) => [nId] -> nId -> [Char] -> IO ()
printIfNodes nIds nId' msg =
  when (nId' `elem` nIds) $
    print $ show nId' ++ " " ++ msg
