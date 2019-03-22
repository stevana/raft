{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

module TestDejaFu where

import Protolude hiding
  (STM, TChan, TVar, newTChan, readMVar, readTChan, writeTChan, atomically, killThread, ThreadId)

import qualified Data.Map as Map
import qualified Data.Sequence as Seq
import qualified Data.Maybe as Maybe
import Test.QuickCheck


import Test.DejaFu hiding (get, ThreadId)
import Test.DejaFu.Internal (Settings(..))
import Test.DejaFu.Conc hiding (ThreadId)
import Test.Tasty
import Test.Tasty.HUnit
import Test.Tasty.DejaFu hiding (get)

import System.Random (mkStdGen, newStdGen)
import Data.List
import TestUtils
import RaftTestT
import qualified SampleEntries

import Raft

dejaFuSettings = defaultSettings { _way = randomly (mkStdGen 42) 100 }

test_concurrency :: [TestTree]
test_concurrency =
    [ testGroup "Leader Election" [ testConcurrentProps (leaderElectionTest node0) mempty ]
    , testGroup "increment(set('x', 41)) == x := 42"
        [ testConcurrentProps incrValue (Map.fromList [("x", 42)], Index 3) ]
    , testGroup "set('x', 0) ... 10x incr(x) == x := 10"
        [ testConcurrentProps multIncrValue (Map.fromList [("x", 10)], Index 12) ]
    , testGroup "Follower redirect with no leader" [ testConcurrentProps followerRedirNoLeader NoLeader ]
    , testGroup "Follower redirect with leader" [ testConcurrentProps followerRedirLeader (CurrentLeader (LeaderId node0)) ]
    , testGroup "New leader election" [ testConcurrentProps newLeaderElection (CurrentLeader (LeaderId node1)) ]
    , testGroup "Comprehensive"
        [ testConcurrentProps comprehensive (Index 14, Map.fromList [("x", 9), ("y", 6), ("z", 42)], CurrentLeader (LeaderId node0)) ]
    ]

testConcurrentProps
  :: (Eq a, Show a)
  => RaftTestClientT ConcIO a
  -> a
  -> TestTree
testConcurrentProps test expected =
  testDejafusWithSettings dejaFuSettings
    [ ("No deadlocks", deadlocksNever)
    , ("No Exceptions", exceptionsNever)
    , ("Success", alwaysTrue (== Right expected))
    ] $ fst <$> withRaftTestNodes emptyTestStates test

leaderElectionTest
  :: NodeId
  -> RaftTestClientM Store
leaderElectionTest nid = leaderElection nid

incrValue :: RaftTestClientM (Store, Index)
incrValue = do
  leaderElection node0
  Right idx <- do
    syncClientWrite node0 (Set "x" 41)
    syncClientWrite node0 (Incr "x")
  Right store <- syncClientRead node0
  pure (store, idx)

multIncrValue :: RaftTestClientM (Store, Index)
multIncrValue = do
    leaderElection node0
    syncClientWrite node0 (Set "x" 0)
    Right idx <-
      fmap (Maybe.fromJust . lastMay) $
        replicateM 10 $ syncClientWrite node0 (Incr "x")
    store <- pollForReadResponse node0
    pure (store, idx)

leaderRedirect :: RaftTestClientM CurrentLeader
leaderRedirect = do
    Left resp <- syncClientWrite node1 (Set "x" 42)
    pure resp

followerRedirNoLeader :: RaftTestClientM CurrentLeader
followerRedirNoLeader = leaderRedirect

followerRedirLeader :: RaftTestClientM CurrentLeader
followerRedirLeader = do
    leaderElection node0
    leaderRedirect

newLeaderElection :: RaftTestClientM CurrentLeader
newLeaderElection = do
    leaderElection node0
    leaderElection node1
    leaderElection node2
    leaderElection node1
    Left ldr <- syncClientRead node0
    pure ldr

comprehensive :: RaftTestClientT ConcIO (Index, Store, CurrentLeader)
comprehensive = do
    leaderElection node0
    Right idx2 <- syncClientWrite node0 (Set "x" 7)
    Right idx3 <- syncClientWrite node0 (Set "y" 3)
    Left (CurrentLeader _) <- syncClientWrite node1 (Incr "y")
    Right _ <- syncClientRead node0

    leaderElection node1
    Right idx5 <- syncClientWrite node1 (Incr "x")
    Right idx6 <- syncClientWrite node1 (Incr "y")
    Right idx7 <- syncClientWrite node1 (Set "z" 40)
    Left (CurrentLeader _) <- syncClientWrite node2 (Incr "y")
    Right _ <- syncClientRead node1

    leaderElection node2
    Right idx9 <- syncClientWrite node2 (Incr "z")
    Right idx10 <- syncClientWrite node2 (Incr "x")
    Left _ <- syncClientWrite node1 (Set "q" 100)
    Right idx11 <- syncClientWrite node2 (Incr "y")
    Left _ <- syncClientWrite node0 (Incr "z")
    Right idx12 <- syncClientWrite node2 (Incr "y")
    Left (CurrentLeader _) <- syncClientWrite node0 (Incr "y")
    Right _ <- syncClientRead node2

    leaderElection node0
    Right idx14 <- syncClientWrite node0 (Incr "z")
    Left (CurrentLeader _) <- syncClientWrite node1 (Incr "y")

    Right store <- syncClientRead node0
    Left ldr <- syncClientRead node1

    pure (idx14, store, ldr)


-- | Check if the majority of the node states are equal after running the given RaftTestClientM
-- program
-- TODO hardcoded to running with 3 nodes
majorityNodeStatesEqual :: RaftTestClientM a -> TestNodeStatesConfig -> TestTree
majorityNodeStatesEqual clientTest startingStatesConfig  =
  testDejafusWithSettings dejaFuSettings
    [ ("No deadlocks", deadlocksNever)
    , ("No exceptions", exceptionsNever)
    , ("Messages exchanged", alwaysTrue checkMessages)
    -- | the test below is commented out as we don't have a good way of
    -- waiting for the majority nodes to catchup to the same state as the leader
    --, ("Correct", alwaysTrue correctResult)
    ] runTest
  where
    runTest :: ConcIO TestNodesResult
    runTest = do
      let startingNodeStates = initTestStates startingStatesConfig
      (res, testNodesResult) <-
        withRaftTestNodes startingNodeStates $ do
          leaderElection node0
          clientTest
      pure testNodesResult

checkMessages :: Either Condition  TestNodesResult -> Bool
checkMessages (Right (_, testRPCMsgEventsSent)) = True
checkMessages (Left _) = True

correctResult :: Either Condition TestNodeStates -> Bool
correctResult (Right testStates) =
  let (inAgreement, inDisagreement) = partition
        (== testNodeLog (testStates Map.! node0))
        (fmap testNodeLog $ Map.elems testStates)
  in  (length inAgreement) > length inDisagreement
correctResult (Left _) = False

test_AEFollower :: TestTree
test_AEFollower =
  testGroup "AEFollower"
    [ majorityNodeStatesEqual (syncClientWrite node0 (Set "x" 7))
      [ (node0, Term 4, SampleEntries.entries)
      , (node1, Term 4, Seq.take 11 SampleEntries.entries)
      , (node2, Term 4, Seq.take 11 SampleEntries.entries)
      ]
    ]

test_AEFollowerBehindOneTerm :: TestTree
test_AEFollowerBehindOneTerm =
  testGroup "AEFollowerBehindOneTerm"
    [ majorityNodeStatesEqual (pure ())
      [ (node0, Term 4, SampleEntries.entries)
      , (node1, Term 3, Seq.take 9 SampleEntries.entries)
      , (node2, Term 3, Seq.take 9 SampleEntries.entries)
      ]
    ]

test_AEFollowerBehindMultipleTerms :: TestTree
test_AEFollowerBehindMultipleTerms =
  testGroup "AEFollowerBehindMultipleTerms"
    [ majorityNodeStatesEqual (pure ())
      [ (node0, Term 4, SampleEntries.entries)
      , (node1, Term 2, Seq.take 6 SampleEntries.entries)
      , (node2, Term 2, Seq.take 6 SampleEntries.entries)
      ]
    ]
