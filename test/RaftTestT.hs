{-# LANGUAGE LiberalTypeSynonyms #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
module RaftTestT where
import Protolude hiding
  (STM, TVar, TChan, newTChan, readMVar, readTChan, writeTChan, atomically, killThread, ThreadId, readTVar, writeTVar)

import Data.Sequence (Seq(..), (><), dropWhileR, (!?))
import qualified Data.Sequence as Seq
import qualified Data.Map as Map
import qualified Data.Serialize as S
import Numeric.Natural

import System.Random (newStdGen)
import Data.Time.Clock.System (getSystemTime)

import Control.Monad.Fail
import Control.Monad.Catch
import Control.Monad.Conc.Class
import Control.Concurrent.Classy.STM.TChan
import Control.Concurrent.Classy.STM.TVar

import Test.DejaFu.Conc (ConcIO)
import Test.Tasty.HUnit

import Raft
import Raft.Client
import Raft.Log
import Raft.Monad

import TestUtils

--------------------------------------------------------------------------------
-- Test State Machine & Commands
--------------------------------------------------------------------------------

type Var = ByteString

data StoreCmd
  = Set Var Natural
  | Incr Var
  deriving (Show, Generic, Eq)

instance S.Serialize StoreCmd

type Store = Map Var Natural

data StoreCtx = StoreCtx

instance RaftStateMachinePure Store StoreCmd where
  data RaftStateMachinePureError Store StoreCmd = StoreError Text deriving (Show)
  type RaftStateMachinePureCtx Store StoreCmd = StoreCtx
  rsmTransition _ store cmd =
    Right $ case cmd of
      Set x n -> Map.insert x n store
      Incr x -> Map.adjust succ x store

instance Monad m => RaftStateMachine (RaftTestT m) Store StoreCmd where
  validateCmd _ = pure (Right ())
  askRaftStateMachinePureCtx = pure StoreCtx

type TestEventChan m = TChan (STM m) (Event StoreCmd)
type TestEventChans m = Map NodeId (TestEventChan m)

type TestClientRespChan m = TChan (STM m) (ClientResponse Store StoreCmd)
type TestClientRespChans m = Map ClientId (TestClientRespChan m)

-- | Node specific state
data TestNodeState = TestNodeState
  { testNodeLog :: Entries StoreCmd
  , testNodePersistentState :: PersistentState
  }
  deriving (Show, Eq)

-- | A map of node ids to their respective node data
type TestNodeStates = Map NodeId TestNodeState

-- | Node specific environment
data TestNodeEnv m = TestNodeEnv
  { testNodeEventChans :: TestEventChans m
  , testClientRespChans :: TestClientRespChans m
  , testNodeStates :: TVar (STM m) TestNodeStates
  , testRaftNodeConfig :: RaftNodeConfig
  }

newtype RaftTestT m a = RaftTestT {
    unRaftTestT :: ReaderT (TestNodeEnv m) m a
  } deriving (Functor, Applicative, Monad, MonadIO, MonadReader (TestNodeEnv m), MonadFail)

type RaftTestM = RaftTestT ConcIO

deriving instance MonadThrow m => MonadThrow (RaftTestT m)
deriving instance MonadCatch m => MonadCatch (RaftTestT m)
deriving instance MonadMask m => MonadMask (RaftTestT m)
deriving instance MonadConc m => MonadConc (RaftTestT m)

runRaftTestT :: Monad m => TestNodeEnv m -> RaftTestT m a -> m a
runRaftTestT testEnv =
  flip runReaderT testEnv . unRaftTestT

newtype RaftTestError = RaftTestError Text
  deriving (Show)

instance Exception RaftTestError

throwTestErr :: MonadConc m => Text -> m a
throwTestErr = throw . RaftTestError

askSelfNodeId :: Monad m => RaftTestT m NodeId
askSelfNodeId = asks (raftConfigNodeId . testRaftNodeConfig)

lookupNodeEventChan :: forall m. MonadConc m => NodeId -> RaftTestT m (TestEventChan m)
lookupNodeEventChan nid = do
  testChanMap :: TestEventChans m <- asks testNodeEventChans
  case Map.lookup nid testChanMap of
    Nothing -> throwTestErr $ "Node id " <> show nid <> " does not exist in TestEnv"
    Just testChan -> pure testChan

getNodeState :: MonadConc m => NodeId -> RaftTestT m TestNodeState
getNodeState nid = do
  testState <- readTVarConc =<< asks testNodeStates
  case Map.lookup nid testState of
    Nothing -> throwTestErr $ "Node id " <> show nid <> " does not exist in TestNodeStates"
    Just testNodeState -> pure testNodeState

modifyNodeState :: MonadConc m =>  NodeId -> (TestNodeState -> TestNodeState) -> RaftTestT m ()
modifyNodeState nid f = do
  testStateTVar <- asks testNodeStates
  atomically $ modifyTVar' testStateTVar $ \testState ->
    case Map.lookup nid testState of
      Nothing -> panic $ "Node id " <> show nid <> " does not exist in TestNodeStates"
      Just testNodeState -> Map.insert nid (f testNodeState) testState

instance MonadConc m => RaftPersist (RaftTestT m) where
  type RaftPersistError (RaftTestT m) = RaftTestError
  initializePersistentState = pure (Right ())
  writePersistentState pstate' = do
    nid <- askSelfNodeId
    testStateTVar <- asks testNodeStates
    fmap Right $ atomically $ modifyTVar' testStateTVar $ \testState ->
      case Map.lookup nid testState of
        Nothing -> testState
        Just testNodeState -> do
          let newTestNodeState = testNodeState { testNodePersistentState = pstate' }
          Map.insert nid newTestNodeState testState
  readPersistentState = do
    nid <- askSelfNodeId
    testState <- readTVarConc =<< asks testNodeStates
    case Map.lookup nid testState of
      Nothing -> pure $ Left (RaftTestError "Failed to find node in environment")
      Just testNodeState -> pure $ Right (testNodePersistentState testNodeState)

instance forall m. MonadConc m => RaftSendRPC (RaftTestT m) StoreCmd where
  sendRPC nid rpc = do
    eventChan <- lookupNodeEventChan nid
    atomically $ writeTChan eventChan (MessageEvent (RPCMessageEvent rpc))

instance MonadConc m => RaftSendClient (RaftTestT m) Store StoreCmd where
  sendClient cid cr = do
    clientRespChans <- asks testClientRespChans
    case Map.lookup cid clientRespChans of
      Nothing -> panic "Failed to find client id in environment"
      Just clientRespChan -> atomically (writeTChan clientRespChan cr)

instance Monad m => RaftInitLog (RaftTestT m) StoreCmd where
  type RaftInitLogError (RaftTestT m) = RaftTestError
  -- No log initialization needs to be done here, everything is in memory.
  initializeLog _ = pure (Right ())

instance MonadConc m => RaftWriteLog (RaftTestT m) StoreCmd where
  type RaftWriteLogError (RaftTestT m) = RaftTestError
  writeLogEntries entries = do
    nid <- askSelfNodeId
    fmap Right $
      modifyNodeState nid $ \testNodeState ->
        let log = testNodeLog testNodeState
         in testNodeState { testNodeLog = log >< entries }

instance MonadConc m => RaftDeleteLog (RaftTestT m) StoreCmd where
  type RaftDeleteLogError (RaftTestT m) = RaftTestError
  deleteLogEntriesFrom idx = do
    nid <- askSelfNodeId
    fmap (const $ Right DeleteSuccess) $
      modifyNodeState nid $ \testNodeState ->
        let log = testNodeLog testNodeState
            newLog = dropWhileR ((<=) idx . entryIndex) log
         in testNodeState { testNodeLog = newLog }

instance MonadConc m => RaftReadLog (RaftTestT m) StoreCmd where
  type RaftReadLogError (RaftTestT m) = RaftTestError
  readLogEntry (Index idx)
    | idx <= 0 = pure $ Right Nothing
    | otherwise = do
        log <- fmap testNodeLog . getNodeState =<< askSelfNodeId
        case log !? fromIntegral (pred idx) of
          Nothing -> pure (Right Nothing)
          Just e
            | entryIndex e == Index idx -> pure (Right $ Just e)
            | otherwise -> pure $ Left (RaftTestError "Malformed log")
  readLastLogEntry = do
    log <- fmap testNodeLog . getNodeState =<< askSelfNodeId
    case log of
      Empty -> pure (Right Nothing)
      _ :|> lastEntry -> pure (Right (Just lastEntry))

instance forall m. (MonadConc m, MonadRaftChan StoreCmd m) => MonadRaftChan StoreCmd (RaftTestT m) where
  type RaftEventChan StoreCmd (RaftTestT m) = TestEventChan m
  readRaftChan = atomically . readTChan
  writeRaftChan chan ev = do
    atomically $ writeTChan chan ev
  newRaftChan = atomically newTChan

instance (MonadConc m, MonadRaftFork m) => MonadRaftFork (RaftTestT m) where
  type RaftThreadId (RaftTestT m) = RaftThreadId m
  raftFork r m = do
    testNodeEnv <- ask
    RaftTestT . lift $ raftFork r (runRaftTestT testNodeEnv m)

--------------------------------------------------------------------------------

data TestClientEnv m = TestClientEnv
  { testClientEnvRespChan :: TestClientRespChan m
  , testClientEnvNodeEventChans :: TestEventChans m
  }

type RaftTestClientT' m = ReaderT (TestClientEnv m) m
type RaftTestClientT m = RaftClientT Store StoreCmd (RaftTestClientT' m)
type RaftTestClientM  = RaftTestClientT ConcIO

instance (MonadConc m, MonadFail m) => RaftClientSend (RaftTestClientT' m) StoreCmd where
  type RaftClientSendError (RaftTestClientT' m) StoreCmd = ()
  raftClientSend nid creq = do
    Just nodeEventChan <- asks (Map.lookup nid . testClientEnvNodeEventChans)
    lift $ atomically $ writeTChan nodeEventChan (MessageEvent (ClientRequestEvent creq))
    pure (Right ())

instance MonadConc m => RaftClientRecv (RaftTestClientT' m) Store StoreCmd where
  type RaftClientRecvError (RaftTestClientT' m) Store = ()
  raftClientRecv = do
    clientRespChan <- asks testClientEnvRespChan
    e <- fmap Right $ lift $ atomically $ readTChan clientRespChan
    return e

runRaftTestClientT
  :: (MonadConc m, MonadIO m)
  => ClientId
  -> TestClientRespChan m
  -> TestEventChans m
  -> RaftTestClientT m a
  -> m a
runRaftTestClientT cid chan chans rtcm = do
  raftClientState <- initRaftClientState mempty <$> liftIO newStdGen
  let raftClientEnv = RaftClientEnv cid
      testClientEnv = TestClientEnv chan chans
   in flip runReaderT testClientEnv
    . runRaftClientT raftClientEnv raftClientState { raftClientRaftNodes = Map.keysSet chans }
    $ rtcm

--------------------------------------------------------------------------------

initTestChanMaps :: MonadConc m => m (Map NodeId (TestEventChan m), Map ClientId (TestClientRespChan m))
initTestChanMaps = do
  eventChans <-
    Map.fromList . zip (toList nodeIds) <$>
      atomically (replicateM (length nodeIds) newTChan)
  clientRespChans <-
    Map.fromList . zip [client0] <$>
      atomically (replicateM 1 newTChan)
  pure (eventChans, clientRespChans)

emptyTestStates :: TestNodeStates
emptyTestStates = Map.fromList $ zip (toList nodeIds) $
      replicate (length nodeIds) (TestNodeState mempty initPersistentState)

initRaftTestEnvs
  :: MonadConc m
  => Map NodeId (TestEventChan m)
  -> Map ClientId (TestClientRespChan m)
  -> TVar (STM m) TestNodeStates
  -> [TestNodeEnv m]
initRaftTestEnvs eventChans clientRespChans testStatesTVar =
  map (TestNodeEnv eventChans clientRespChans testStatesTVar) testConfigs

runTestNode
  :: ( Typeable m
     , MonadConc m
     , MonadIO m
     , MonadRaftFork m
     , MonadFail m
     , MonadRaftChan StoreCmd m
     )
  => TestNodeEnv m
  -> m ()
runTestNode testEnv =
    runRaftTestT testEnv $ do
      raftEnv <- initializeRaftEnv eventChan dummyTimer dummyTimer (testRaftNodeConfig testEnv) NoLogs
      runRaftT initRaftNodeState raftEnv $
        handleEventLoop (mempty :: Store)
  where
    nid = raftConfigNodeId (testRaftNodeConfig testEnv)
    Just eventChan = Map.lookup nid (testNodeEventChans testEnv)
    dummyTimer = pure ()

forkTestNodes
  :: ( Typeable m
     , MonadConc m
     , MonadIO m
     , MonadRaftFork m
     , MonadFail m
     , MonadRaftChan StoreCmd m
     )
  => [TestNodeEnv m]
  -> m [ThreadId m]
forkTestNodes testEnvs =
  mapM (fork . runTestNode) testEnvs

--------------------------------------------------------------------------------
-- Helpers
--------------------------------------------------------------------------------

-- | This function can be safely "run" without worry about impacting the client
-- SerialNum of the client requests.
--
-- Warning: If read requests start to include serial numbers, this function will
-- no longer be safe to `runRaftTestClientT` on.
pollForReadResponse :: (MonadConc m, MonadIO m, MonadFail m) => NodeId -> RaftTestClientT m Store
pollForReadResponse nid = do
  eRes <- clientReadFrom nid ClientReadStateMachine
  case eRes of
    -- TODO Handle other cases of 'ClientReadResp'
    Right (ClientReadRespStateMachine res) -> do
      pure res
    e -> do
      liftIO $ Control.Monad.Conc.Class.threadDelay 10000
      pollForReadResponse nid

syncClientRead :: (MonadConc m, MonadIO m, MonadFail m) => NodeId -> RaftTestClientT m (Either CurrentLeader Store)
syncClientRead nid = do
  eRes <- clientReadFrom nid ClientReadStateMachine
  case eRes of
    -- TODO Handle other cases of 'ClientReadResp'
    Right (ClientReadRespStateMachine store) -> pure $ Right store
    Left (RaftClientUnexpectedRedirect (ClientRedirResp ldr)) -> pure $ Left ldr
    _ -> panic "Failed to recieve valid read response"

syncClientWrite
  :: (MonadConc m, MonadIO m, MonadFail m)
  => NodeId
  -> StoreCmd
  -> RaftTestClientT m (Either CurrentLeader Index)
syncClientWrite nid cmd = do
  eRes <- clientWriteTo nid cmd
  case eRes of
    Right (ClientWriteRespSuccess idx sn) -> do
      Just nodeEventChan <- lift (asks (Map.lookup nid . testClientEnvNodeEventChans))
      pure $ Right idx
    Left (RaftClientUnexpectedRedirect (ClientRedirResp ldr)) -> pure $ Left ldr
    a -> do
      print a
      panic "Failed to receive client write response..."

heartbeat :: (MonadConc m, MonadIO m) => TestEventChan m -> m ()
heartbeat eventChan = do
  sysTime <- liftIO getSystemTime
  atomically $ writeTChan eventChan (TimeoutEvent sysTime HeartbeatTimeout)

clientReadReq :: ClientId -> Event StoreCmd
clientReadReq cid = MessageEvent $ ClientRequestEvent $ ClientRequest cid (ClientReadReq ClientReadStateMachine)

clientReadRespChan :: MonadConc m => RaftTestClientT m (ClientResponse Store StoreCmd)
clientReadRespChan = do
  clientRespChan <- lift (asks testClientEnvRespChan)
  lift $ lift $ atomically $ readTChan clientRespChan

leaderElection
  :: (MonadConc m, MonadIO m, MonadFail m)
  => NodeId
  -> RaftTestClientT m Store
leaderElection nid = do
  leaderElection' nid
  pollForReadResponse nid

leaderElection' :: (MonadConc m, MonadIO m, MonadFail m) => NodeId -> RaftTestClientT m ()
leaderElection' nid = do
  eventChans <- lift $ asks testClientEnvNodeEventChans
  let Just nodeEventChan = Map.lookup nid eventChans
  sysTime <- liftIO getSystemTime
  lift $ lift $ atomically $ writeTChan nodeEventChan (TimeoutEvent sysTime ElectionTimeout)

--------------------------------------------------------------------------------
-- Test Harness and helpers
--------------------------------------------------------------------------------

type TestNodeStatesConfig =  [(NodeId, Term, Entries StoreCmd)]

withRaftTestNodes
  :: ( Typeable m
     , MonadConc m
     , MonadIO m
     , MonadRaftFork m
     , MonadFail m
     , MonadRaftChan StoreCmd m
     )
  => TestNodeStates
  -> RaftTestClientT m a
  -> m (a, TestNodeStates)
withRaftTestNodes startingNodeStates raftTest =
  Control.Monad.Catch.bracket setup teardown
    $ \(tids, (eventChans, clientRespChans, testNodeStatesTVar)) -> do
        let Just client0RespChan = Map.lookup client0 clientRespChans
        res <- runRaftTestClientT client0 client0RespChan eventChans raftTest

        testStates <- readTVarConc testNodeStatesTVar
        pure (res, testStates)

 where
  setup = do
    (eventChans, clientRespChans) <- initTestChanMaps
    testNodeStatesTVar <- atomically $ newTVar startingNodeStates
    let testNodeEnvs =
          initRaftTestEnvs eventChans clientRespChans testNodeStatesTVar
    tids <- forkTestNodes testNodeEnvs
    pure (tids, (eventChans, clientRespChans, testNodeStatesTVar))
  teardown = mapM_ killThread . fst

initTestStates :: TestNodeStatesConfig -> TestNodeStates
initTestStates startingValues =
  foldl adjustTestStates emptyTestStates startingValues
 where
  adjustTestStates nodeState (node, term, entries) =
    Map.adjust (adjustSingle entries term) node nodeState

  adjustSingle
    :: Entries StoreCmd -> Term -> TestNodeState -> TestNodeState
  adjustSingle entries term nodeState = nodeState
    { testNodeLog             = entries
    , testNodePersistentState = PersistentState
      { currentTerm = term
      , votedFor    = Nothing
      }
    }
