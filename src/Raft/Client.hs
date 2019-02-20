{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE UndecidableInstances #-}

module Raft.Client
(
  -- ** Raft Interface
  RaftSendClient(..)
, RaftRecvClient(..)

, SerialNum(..)

-- ** Client Requests
, ClientRequest(..)
, ClientReq(..)
, ClientReadReq(..)
, ReadEntriesSpec(..)

-- ** Client Responses
, ClientResponse(..)

, ClientRespSpec(..)
, ClientReadRespSpec(..)
, ClientWriteRespSpec(..)

, ClientReadResp(..)
, ClientWriteResp(..)
, ClientRedirResp(..)


-- ** Client Interface
, RaftClientSend(..)
, RaftClientRecv(..)

, RaftClientState(..)
, RaftClientEnv(..)
, initRaftClientState

, RaftClientT
, runRaftClientT

, RaftClientError(..)
, clientRead
, clientReadFrom
, clientReadTimeout

, clientWrite
, clientWriteTo
, clientWriteTimeout

, clientSend
, clientRecv

, retryOnRedirect

, clientAddNode
, clientGetNodes

) where

import Protolude hiding (threadDelay, STM, )

import Control.Concurrent.Lifted (threadDelay)

import Control.Monad.Base
import Control.Monad.Catch
import Control.Monad.Fail
import Control.Monad.Trans.Class
import Control.Monad.Trans.Control


import qualified Data.Set as Set
import qualified Data.Serialize as S

import System.Random
import System.Console.Haskeline.MonadException (MonadException(..), RunIO(..))
import System.Timeout.Lifted (timeout)

import Raft.Log (Entry, Entries, ReadEntriesSpec)
import Raft.StateMachine
import Raft.Types

--------------------------------------------------------------------------------
-- Raft Interface
--------------------------------------------------------------------------------

{- This is the interface with which Raft nodes interact with client programs -}

-- | Interface for Raft nodes to send messages to clients
--
-- TODO It would be really nice if 'RSMP' was a superclass, but currently this
-- can't happen because of cyclic imports.
class RaftSendClient m sm v where
  sendClient :: ClientId -> ClientResponse sm v -> m ()

-- | Interface for Raft nodes to receive messages from clients
class Show (RaftRecvClientError m v) => RaftRecvClient m v where
  type RaftRecvClientError m v
  receiveClient :: m (Either (RaftRecvClientError m v) (ClientRequest v))

--------------------------------------------------------------------------------
-- Client Requests
--------------------------------------------------------------------------------

-- | Representation of a client request coupled with the client id
data ClientRequest v
  = ClientRequest ClientId (ClientReq v)
  deriving (Show, Generic)

instance S.Serialize v => S.Serialize (ClientRequest v)

-- | Representation of a client request
data ClientReq v
  = ClientReadReq ClientReadReq -- ^ Request the latest state of the state machine
  | ClientWriteReq SerialNum v -- ^ Write a command
  deriving (Show, Generic)

instance S.Serialize v => S.Serialize (ClientReq v)

data ClientReadReq
  = ClientReadEntries ReadEntriesSpec
  | ClientReadStateMachine
  deriving (Show, Generic, S.Serialize)

--------------------------------------------------------------------------------
-- Client Responses
--------------------------------------------------------------------------------

-- | Specification for the data inside a ClientResponse
data ClientRespSpec sm v
  = ClientReadRespSpec (ClientReadRespSpec sm)
  | ClientWriteRespSpec (ClientWriteRespSpec sm v)
  | ClientRedirRespSpec CurrentLeader
  deriving (Generic)

deriving instance (Show sm, Show v, Show (RaftStateMachinePureError sm v)) => Show (ClientRespSpec sm v)
deriving instance (S.Serialize sm, S.Serialize v, S.Serialize (RaftStateMachinePureError sm v)) => S.Serialize (ClientRespSpec sm v)

data ClientReadRespSpec sm
  = ClientReadRespSpecEntries ReadEntriesSpec
  | ClientReadRespSpecStateMachine sm
  deriving (Show, Generic, S.Serialize)

data ClientWriteRespSpec sm v
  = ClientWriteRespSpecSuccess Index SerialNum
  | ClientWriteRespSpecFail SerialNum (RaftStateMachinePureError sm v)
  deriving (Generic)

deriving instance (Show sm, Show v, Show (RaftStateMachinePureError sm v)) => Show (ClientWriteRespSpec sm v)
deriving instance (S.Serialize sm, S.Serialize v, S.Serialize (RaftStateMachinePureError sm v)) => S.Serialize (ClientWriteRespSpec sm v)

--------------------------------------------------------------------------------

-- | The datatype sent back to the client as an actual response
data ClientResponse sm v
  = ClientReadResponse (ClientReadResp sm v)
    -- ^ Respond with the latest state of the state machine.
  | ClientWriteResponse (ClientWriteResp sm v)
    -- ^ Respond with the index of the entry appended to the log
  | ClientRedirectResponse ClientRedirResp
    -- ^ Respond with the node id of the current leader
  deriving (Generic)

deriving instance (Show sm, Show v, Show (ClientWriteResp sm v)) => Show (ClientResponse sm v)
deriving instance (S.Serialize sm, S.Serialize v, S.Serialize (ClientWriteResp sm v)) => S.Serialize (ClientResponse sm v)

-- | Representation of a read response to a client
data ClientReadResp sm v
  = ClientReadRespStateMachine sm
  | ClientReadRespEntry (Entry v)
  | ClientReadRespEntries (Entries v)
  deriving (Show, Generic, S.Serialize)

-- | Representation of a write response to a client
data ClientWriteResp sm v
  = ClientWriteRespSuccess Index SerialNum
  -- ^ Index of the entry appended to the log due to the previous client request
  | ClientWriteRespFail SerialNum (RaftStateMachinePureError sm v)
  deriving (Generic)

deriving instance (Show sm, Show v, Show (RaftStateMachinePureError sm v)) => Show (ClientWriteResp sm v)
deriving instance (S.Serialize sm, S.Serialize v, S.Serialize (RaftStateMachinePureError sm v)) => S.Serialize (ClientWriteResp sm v)

-- | Representation of a redirect response to a client
data ClientRedirResp
  = ClientRedirResp CurrentLeader
  deriving (Show, Generic, S.Serialize)

--------------------------------------------------------------------------------
-- Client Interface
--------------------------------------------------------------------------------

{- This is the interface with which clients interact with Raft nodes -}

class Monad m => RaftClientSend m v where
  type RaftClientSendError m v
  raftClientSend :: NodeId -> ClientRequest v -> m (Either (RaftClientSendError m v) ())

class Monad m => RaftClientRecv m sm v | m sm -> v where
  type RaftClientRecvError m sm
  raftClientRecv :: m (Either (RaftClientRecvError m sm) (ClientResponse sm v))

-- | Each client may have at most one command outstanding at a time and
-- commands must be dispatched in serial number order.
data RaftClientState = RaftClientState
  { raftClientCurrentLeader :: CurrentLeader
  , raftClientSerialNum :: SerialNum
  , raftClientRaftNodes :: Set NodeId
  , raftClientRandomGen :: StdGen
  }

data RaftClientEnv = RaftClientEnv
  { raftClientId :: ClientId
  }

initRaftClientState :: Set NodeId -> StdGen -> RaftClientState
initRaftClientState = RaftClientState NoLeader 0

newtype RaftClientT s v m a = RaftClientT
  { unRaftClientT :: ReaderT RaftClientEnv (StateT RaftClientState m) a
  } deriving newtype (Functor, Applicative, Monad, MonadIO, MonadState RaftClientState, MonadReader RaftClientEnv, MonadFail, Alternative, MonadPlus)

deriving newtype instance MonadThrow m => MonadThrow (RaftClientT s v m)
deriving newtype instance MonadCatch m => MonadCatch (RaftClientT s v m)
deriving newtype instance MonadMask m => MonadMask (RaftClientT s v m)

instance MonadTrans (RaftClientT s v) where
  lift = RaftClientT . lift . lift

deriving newtype instance MonadBase IO m => MonadBase IO (RaftClientT s v m)

instance MonadTransControl (RaftClientT s v) where
    type StT (RaftClientT s v) a = StT (ReaderT RaftClientEnv) (StT (StateT RaftClientState) a)
    liftWith = defaultLiftWith2 RaftClientT unRaftClientT
    restoreT = defaultRestoreT2 RaftClientT

-- These instances are for use of the 'timeout' function that requires a
-- MonadBaseControl IO constraint.
--
-- TODO is this still necessary after the removal of MonadConc?
instance (MonadBaseControl IO m) => MonadBaseControl IO (RaftClientT s v m) where
    type StM (RaftClientT s v m) a = ComposeSt (RaftClientT s v) m a
    liftBaseWith    = defaultLiftBaseWith
    restoreM        = defaultRestoreM

-- This annoying instance is because of the Haskeline library, letting us use a
-- custom monad transformer stack as the base monad of 'InputT'. IMO it should
-- be automatically derivable. Why does haskeline use a custom exception
-- monad... ?
instance MonadException m => MonadException (RaftClientT s v m) where
  controlIO f =
    RaftClientT $ ReaderT $ \r -> StateT $ \s ->
      controlIO $ \(RunIO run) ->
        let run' = RunIO (fmap (RaftClientT . ReaderT . const . StateT . const) . run . flip runStateT s . flip runReaderT r . unRaftClientT)
         in fmap (flip runStateT s . flip runReaderT r . unRaftClientT) $ f run'

instance RaftClientSend m v => RaftClientSend (RaftClientT s v m) v where
  type RaftClientSendError (RaftClientT s v m) v = RaftClientSendError m v
  raftClientSend nid creq = lift (raftClientSend nid creq)

instance RaftClientRecv m s v => RaftClientRecv (RaftClientT s v m) s v where
  type RaftClientRecvError (RaftClientT s v m) s = RaftClientRecvError m s
  raftClientRecv = lift raftClientRecv

runRaftClientT :: Monad m => RaftClientEnv -> RaftClientState -> RaftClientT s v m a -> m a
runRaftClientT raftClientEnv raftClientState =
  flip evalStateT raftClientState . flip runReaderT raftClientEnv . unRaftClientT

--------------------------------------------------------------------------------

data RaftClientError s v m where
  RaftClientSendError  :: RaftClientSendError m v -> RaftClientError s v m
  RaftClientRecvError  :: RaftClientRecvError m s -> RaftClientError s v m
  RaftClientTimeout    :: Text -> RaftClientError s v m
  RaftClientUnexpectedReadResp  :: ClientReadResp s v -> RaftClientError s v m
  RaftClientUnexpectedWriteResp :: ClientWriteResp s v -> RaftClientError s v m
  RaftClientUnexpectedRedirect  :: ClientRedirResp -> RaftClientError s v m

deriving instance (Show s, Show v, Show (RaftClientSendError m v), Show (RaftClientRecvError m s), Show (RaftStateMachinePureError s v)) => Show (RaftClientError s v m)

-- | Send a read request to the curent leader and wait for a response
clientRead
  :: (RaftClientSend m v, RaftClientRecv m s v)
  => ClientReadReq
  -> RaftClientT s v m (Either (RaftClientError s v m) (ClientReadResp s v))
clientRead crr = do
  eSend <- clientSendRead crr
  case eSend of
    Left err -> pure (Left (RaftClientSendError err))
    Right _ -> clientRecvRead

-- | Send a read request to a specific raft node, regardless of leader, and wait
-- for a response.
clientReadFrom
  :: (RaftClientSend m v, RaftClientRecv m s v)
  => NodeId
  -> ClientReadReq
  -> RaftClientT s v m (Either (RaftClientError s v m) (ClientReadResp s v))
clientReadFrom nid crr = do
  eSend <- clientSendReadTo nid crr
  case eSend of
    Left err -> pure (Left (RaftClientSendError err))
    Right _ -> clientRecvRead

-- | 'clientRead' but with a timeout
clientReadTimeout
  :: (MonadBaseControl IO m, RaftClientSend m v, RaftClientRecv m s v)
  => Int
  -> ClientReadReq
  -> RaftClientT s v m (Either (RaftClientError s v m) (ClientReadResp s v))
clientReadTimeout t = clientTimeout "clientRead" t . clientRead

-- | Send a write request to the current leader and wait for a response
clientWrite
  :: (RaftClientSend m v, RaftClientRecv m s v)
  => v
  -> RaftClientT s v m (Either (RaftClientError s v m) (ClientWriteResp s v))
clientWrite cmd = do
  eSend <- clientSendWrite cmd
  case eSend of
    Left err -> pure (Left (RaftClientSendError err))
    Right _ -> clientRecvWrite

-- | Send a read request to a specific raft node, regardless of leader, and wait
-- for a response.
clientWriteTo
  :: (RaftClientSend m v, RaftClientRecv m s v)
  => NodeId
  -> v
  -> RaftClientT s v m (Either (RaftClientError s v m) (ClientWriteResp s v))
clientWriteTo nid cmd = do
  eSend <- clientSendWriteTo nid cmd
  case eSend of
    Left err -> pure (Left (RaftClientSendError err))
    Right _ -> clientRecvWrite

clientWriteTimeout
  :: (MonadBaseControl IO m, RaftClientSend m v, RaftClientRecv m s v)
  => Int
  -> v
  -> RaftClientT s v m (Either (RaftClientError s v m) (ClientWriteResp s v))
clientWriteTimeout t cmd = clientTimeout "clientWrite" t (clientWrite cmd)

clientTimeout
  :: (MonadBaseControl IO m, RaftClientSend m v, RaftClientRecv m s v)
  => Text
  -> Int
  -> RaftClientT s v m (Either (RaftClientError s v m) r)
  -> RaftClientT s v m (Either (RaftClientError s v m) r)
clientTimeout fnm t r = do
  mRes <- timeout t r
  case mRes of
    Nothing -> pure (Left (RaftClientTimeout fnm))
    Just (Left err) -> pure (Left err)
    Just (Right cresp) -> pure (Right cresp)

-- | Given a blocking client send/receive, retry if the received value is not
-- expected
retryOnRedirect
  :: MonadBaseControl IO m
  => RaftClientT s v m (Either (RaftClientError s v m) r)
  -> RaftClientT s v m (Either (RaftClientError s v m) r)
retryOnRedirect action = do
  eRes <- action
  case eRes of
    Left (RaftClientUnexpectedRedirect _) -> do
      threadDelay 10000
      retryOnRedirect action
    Left err -> pure (Left err)
    Right resp -> pure (Right resp)

--------------------------------------------------------------------------------
-- Helpers
--------------------------------------------------------------------------------

-- | Send a read request to the current leader. Nonblocking.
clientSendRead
  :: RaftClientSend m v
  => ClientReadReq
  -> RaftClientT s v m (Either (RaftClientSendError m v) ())
clientSendRead crr =
  clientSend (ClientReadReq crr)

clientSendReadTo
  :: RaftClientSend m v
  => NodeId
  -> ClientReadReq
  -> RaftClientT s v m (Either (RaftClientSendError m v) ())
clientSendReadTo nid crr =
  clientSendTo nid (ClientReadReq crr)

-- | Send a write request to the current leader. Nonblocking.
clientSendWrite
  :: RaftClientSend m v
  => v
  -> RaftClientT s v m (Either (RaftClientSendError m v) ())
clientSendWrite v = do
  gets raftClientSerialNum >>= \sn ->
    clientSend (ClientWriteReq sn v)

-- | Send a write request to a specific raft node, ignoring the current
-- leader. This function is used in testing.
clientSendWriteTo
  :: RaftClientSend m v
  => NodeId
  -> v
  -> RaftClientT s v m (Either (RaftClientSendError m v) ())
clientSendWriteTo nid v =
  gets raftClientSerialNum >>= \sn ->
    clientSendTo nid (ClientWriteReq sn v)

-- | Send a request to the current leader. Nonblocking.
clientSend
  :: (RaftClientSend m v)
  => ClientReq v
  -> RaftClientT s v m (Either (RaftClientSendError m v) ())
clientSend creq = do
  currLeader <- gets raftClientCurrentLeader
  case currLeader of
    NoLeader -> clientSendRandom creq
    CurrentLeader (LeaderId nid) -> do
      cid <- asks raftClientId
      eRes <- raftClientSend nid (ClientRequest cid creq)
      case eRes of
        Left err -> clientSendRandom creq
        Right resp -> pure (Right resp)

-- | Send a request to a specific raft node, ignoring the current leader.
-- This function is used in testing.
clientSendTo
  :: RaftClientSend m v
  => NodeId
  -> ClientReq v
  -> RaftClientT s v m (Either (RaftClientSendError m v) ())
clientSendTo nid creq = do
  cid <- asks raftClientId
  raftClientSend nid (ClientRequest cid creq)

-- | Send a request to a random node.
-- This function is used if there is no leader.
clientSendRandom
  :: RaftClientSend m v
  => ClientReq v
  -> RaftClientT s v m (Either (RaftClientSendError m v) ())
clientSendRandom creq = do
  cid <- asks raftClientId
  raftNodes <- gets raftClientRaftNodes
  randomGen <- gets raftClientRandomGen
  let (idx, newRandomGen) = randomR (0, length raftNodes - 1) randomGen
  case atMay (toList raftNodes) idx of
    Nothing -> panic "No raft nodes known by client"
    Just nid -> do
      modify $ \s -> s { raftClientRandomGen = newRandomGen }
      raftClientSend nid (ClientRequest cid creq)

--------------------------------------------------------------------------------

-- | Waits for a write response on the client socket
-- Warning: This function discards unexpected read and redirect responses
clientRecvWrite
  :: (RaftClientSend m v, RaftClientRecv m s v)
  => RaftClientT s v m (Either (RaftClientError s v m) (ClientWriteResp s v))
clientRecvWrite = do
  eRes <- clientRecv
  case eRes of
    Left err -> pure (Left (RaftClientRecvError err))
    Right cresp ->
      case cresp of
        ClientRedirectResponse crr -> pure (Left (RaftClientUnexpectedRedirect crr))
        ClientReadResponse crr -> pure (Left (RaftClientUnexpectedReadResp crr))
        ClientWriteResponse cwr -> pure (Right cwr)

-- | Waits for a read response on the client socket
-- Warning: This function discards unexpected write and redirect responses
clientRecvRead
  :: (RaftClientSend m v, RaftClientRecv m s v)
  => RaftClientT s v m (Either (RaftClientError s v m) (ClientReadResp s v))
clientRecvRead = do
  eRes <- clientRecv
  case eRes of
    Left err -> pure (Left (RaftClientRecvError err))
    Right cresp ->
      case cresp of
        ClientRedirectResponse crr -> pure (Left (RaftClientUnexpectedRedirect crr))
        ClientWriteResponse cwr -> pure (Left (RaftClientUnexpectedWriteResp cwr))
        ClientReadResponse crr -> pure (Right crr)

-- | Wait for a response from the current leader.
-- This function handles leader changes and write request serial numbers.
clientRecv
  :: RaftClientRecv m s v
  => RaftClientT s v m (Either (RaftClientRecvError m s) (ClientResponse s v))
clientRecv = do
  ecresp <- raftClientRecv
  case ecresp of
    Left err -> pure (Left err)
    Right cresp ->
      case cresp of
        ClientWriteResponse cwr ->
          case cwr of
            ClientWriteRespSuccess _ serial@(SerialNum n) -> do
              SerialNum m <- gets raftClientSerialNum
              if m == n
              then do
                modify $ \s -> s
                  { raftClientSerialNum = SerialNum (succ m) }
                pure (Right cresp)
              else handleLowerSerialNum serial
            ClientWriteRespFail serial@(SerialNum n) err -> do
              SerialNum m <- gets raftClientSerialNum
              if m == n
              then pure (Right cresp)
              else handleLowerSerialNum serial

        ClientRedirectResponse (ClientRedirResp currLdr) -> do
          modify $ \s -> s
            { raftClientCurrentLeader = currLdr }
          pure (Right cresp)
        _ -> pure (Right cresp)
  where
    -- Here, we ignore the response if we are receiving a response
    -- regarding a previously committed write request. This regularly
    -- happens when a write request is submitted, committed, and
    -- responded to, but the leader subsequently dies before letting all
    -- other nodes know that the write request has been committed.
    handleLowerSerialNum (SerialNum n) = do
      SerialNum m <- gets raftClientSerialNum
      if n < m
      then clientRecv
      else do
        let errMsg = "Received invalid serial number response: Expected " <> show m <> " but got " <> show n
        panic $ errMsg


--------------------------------------------------------------------------------

clientAddNode :: Monad m => NodeId -> RaftClientT s v m ()
clientAddNode nid = modify $ \s ->
  s { raftClientRaftNodes = Set.insert nid (raftClientRaftNodes s) }

clientGetNodes :: Monad m => RaftClientT s v m (Set NodeId)
clientGetNodes = gets raftClientRaftNodes
