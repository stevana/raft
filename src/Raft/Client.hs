{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE UndecidableInstances #-}

module Raft.Client
(
  -- ** Raft Interface
  RaftSendClient(..)
, RaftRecvClient(..)

, SerialNum(..)

, ClientRequest(..)
, ClientReq(..)

, ClientResponse(..)
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

, retryOnRedirect

, clientAddNode
, clientGetNodes

) where

import Protolude hiding (threadDelay)

import Control.Concurrent.Classy
import Control.Monad.Base
import Control.Monad.Catch
import Control.Monad.Fail
import Control.Monad.Trans.Class
import Control.Monad.Trans.Control

import qualified Data.Set as Set
import qualified Data.Serialize as S
import Numeric.Natural (Natural)

import System.Random
import System.Console.Haskeline.MonadException (MonadException(..), RunIO(..))
import System.Timeout.Lifted (timeout)

import Raft.Types

--------------------------------------------------------------------------------
-- Raft Interface
--------------------------------------------------------------------------------

{- This is the interface with which Raft nodes interact with client programs -}

-- | Interface for Raft nodes to send messages to clients
class RaftSendClient m sm where
  sendClient :: ClientId -> ClientResponse sm -> m ()

-- | Interface for Raft nodes to receive messages from clients
class Show (RaftRecvClientError m v) => RaftRecvClient m v where
  type RaftRecvClientError m v
  receiveClient :: m (Either (RaftRecvClientError m v) (ClientRequest v))

newtype SerialNum = SerialNum Natural
  deriving (Show, Read, Eq, Ord, Enum, Num, Generic, S.Serialize)

-- | Representation of a client request coupled with the client id
data ClientRequest v
  = ClientRequest ClientId (ClientReq v)
  deriving (Show, Generic)

instance S.Serialize v => S.Serialize (ClientRequest v)

-- | Representation of a client request
data ClientReq v
  = ClientReadReq -- ^ Request the latest state of the state machine
  | ClientWriteReq SerialNum v -- ^ Write a command
  deriving (Show, Generic)

instance S.Serialize v => S.Serialize (ClientReq v)

-- | Representation of a client response
data ClientResponse s
  = ClientReadResponse (ClientReadResp s)
    -- ^ Respond with the latest state of the state machine.
  | ClientWriteResponse ClientWriteResp
    -- ^ Respond with the index of the entry appended to the log
  | ClientRedirectResponse ClientRedirResp
    -- ^ Respond with the node id of the current leader
  deriving (Show, Generic)

instance S.Serialize s => S.Serialize (ClientResponse s)

-- | Representation of a read response to a client
-- The `s` stands for the "current" state of the state machine
newtype ClientReadResp s
  = ClientReadResp s
  deriving (Show, Generic)

instance S.Serialize s => S.Serialize (ClientReadResp s)

-- | Representation of a write response to a client
data ClientWriteResp
  = ClientWriteResp Index SerialNum
  -- ^ Index of the entry appended to the log due to the previous client request
  deriving (Show, Generic)

instance S.Serialize ClientWriteResp

-- | Representation of a redirect response to a client
data ClientRedirResp
  = ClientRedirResp CurrentLeader
  deriving (Show, Generic)

instance S.Serialize ClientRedirResp

--------------------------------------------------------------------------------
-- Client Interface
--------------------------------------------------------------------------------

{- This is the interface with which clients interact with Raft nodes -}

class Monad m => RaftClientSend m v where
  type RaftClientSendError m v
  raftClientSend :: NodeId -> ClientRequest v -> m (Either (RaftClientSendError m v) ())

class Monad m => RaftClientRecv m s where
  type RaftClientRecvError m s
  raftClientRecv :: m (Either (RaftClientRecvError m s) (ClientResponse s))

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

initRaftClientState :: StdGen -> RaftClientState
initRaftClientState = RaftClientState NoLeader 0 mempty

newtype RaftClientT s v m a = RaftClientT
  { unRaftClientT :: ReaderT RaftClientEnv (StateT RaftClientState m) a
  } deriving (Functor, Applicative, Monad, MonadIO, MonadState RaftClientState, MonadReader RaftClientEnv, MonadFail, Alternative, MonadPlus)

deriving instance MonadThrow m => MonadThrow (RaftClientT s v m)
deriving instance MonadCatch m => MonadCatch (RaftClientT s v m)
deriving instance MonadMask m => MonadMask (RaftClientT s v m)
deriving instance MonadSTM m => MonadSTM (RaftClientT s v m)
deriving instance MonadConc m => MonadConc (RaftClientT s v m)

instance MonadTrans (RaftClientT s v) where
  lift = RaftClientT . lift . lift

deriving instance MonadBase IO m => MonadBase IO (RaftClientT s v m)

instance MonadTransControl (RaftClientT s v) where
    type StT (RaftClientT s v) a = StT (ReaderT RaftClientEnv) (StT (StateT RaftClientState) a)
    liftWith = defaultLiftWith2 RaftClientT unRaftClientT
    restoreT = defaultRestoreT2 RaftClientT

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

instance RaftClientRecv m s => RaftClientRecv (RaftClientT s v m) s where
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
  RaftClientUnexpectedReadResp  :: ClientReadResp s -> RaftClientError s v m
  RaftClientUnexpectedWriteResp :: ClientWriteResp -> RaftClientError s v m
  RaftClientUnexpectedRedirect  :: ClientRedirResp -> RaftClientError s v m

deriving instance (Show s, Show (RaftClientSendError m v), Show (RaftClientRecvError m s)) => Show (RaftClientError s v m)

-- | Send a read request to the curent leader and wait for a response
clientRead
  :: (Show (RaftClientSendError m v), RaftClientSend m v, RaftClientRecv m s)
  => RaftClientT s v m (Either (RaftClientError s v m) (ClientReadResp s))
clientRead = do
  eSend <- clientSendRead
  case eSend of
    Left err -> pure (Left (RaftClientSendError err))
    Right _ -> clientRecvRead

-- | Send a read request to a specific raft node, regardless of leader, and wait
-- for a response.
clientReadFrom
  :: (RaftClientSend m v, RaftClientRecv m s)
  => NodeId
  -> RaftClientT s v m (Either (RaftClientError s v m) (ClientReadResp s))
clientReadFrom nid = do
  eSend <- clientSendReadTo nid
  case eSend of
    Left err -> pure (Left (RaftClientSendError err))
    Right _ -> clientRecvRead

-- | 'clientRead' but with a timeout
clientReadTimeout
  :: (MonadBaseControl IO m, Show (RaftClientSendError m v), RaftClientSend m v, RaftClientRecv m s)
  => Int
  -> RaftClientT s v m (Either (RaftClientError s v m) (ClientReadResp s))
clientReadTimeout t = clientTimeout "clientRead" t clientRead

-- | Send a write request to the current leader and wait for a response
clientWrite
  :: (Show (RaftClientSendError m v), RaftClientSend m v, RaftClientRecv m s)
  => v
  -> RaftClientT s v m (Either (RaftClientError s v m) ClientWriteResp)
clientWrite cmd = do
  eSend <- clientSendWrite cmd
  case eSend of
    Left err -> pure (Left (RaftClientSendError err))
    Right _ -> clientRecvWrite

-- | Send a read request to a specific raft node, regardless of leader, and wait
-- for a response.
clientWriteTo
  :: (RaftClientSend m v, RaftClientRecv m s)
  => NodeId
  -> v
  -> RaftClientT s v m (Either (RaftClientError s v m) ClientWriteResp)
clientWriteTo nid cmd = do
  eSend <- clientSendWriteTo nid cmd
  case eSend of
    Left err -> pure (Left (RaftClientSendError err))
    Right _ -> clientRecvWrite

clientWriteTimeout
  :: (MonadBaseControl IO m, Show (RaftClientSendError m v), RaftClientSend m v, RaftClientRecv m s)
  => Int
  -> v
  -> RaftClientT s v m (Either (RaftClientError s v m) ClientWriteResp)
clientWriteTimeout t cmd = clientTimeout "clientWrite" t (clientWrite cmd)

clientTimeout
  :: (MonadBaseControl IO m, RaftClientSend m v, RaftClientRecv m s)
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
  :: MonadConc m
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
  :: (Show (RaftClientSendError m v), RaftClientSend m v)
  => RaftClientT s v m (Either (RaftClientSendError m v) ())
clientSendRead =
  asks raftClientId >>= \cid ->
    clientSend (ClientRequest cid ClientReadReq)

clientSendReadTo
  :: RaftClientSend m v
  => NodeId
  -> RaftClientT s v m (Either (RaftClientSendError m v) ())
clientSendReadTo nid =
  asks raftClientId >>= \cid ->
    clientSendTo nid (ClientRequest cid ClientReadReq)

-- | Send a write request to the current leader. Nonblocking.
clientSendWrite
  :: (Show (RaftClientSendError m v), RaftClientSend m v)
  => v
  -> RaftClientT s v m (Either (RaftClientSendError m v) ())
clientSendWrite v = do
  asks raftClientId >>= \cid -> gets raftClientSerialNum >>= \sn ->
    clientSend (ClientRequest cid (ClientWriteReq sn v))

-- | Send a write request to a specific raft node, ignoring the current
-- leader. This function is used in testing.
clientSendWriteTo
  :: RaftClientSend m v
  => NodeId
  -> v
  -> RaftClientT s v m (Either (RaftClientSendError m v) ())
clientSendWriteTo nid v =
  asks raftClientId >>= \cid -> gets raftClientSerialNum >>= \sn ->
    clientSendTo nid (ClientRequest cid (ClientWriteReq sn v))

-- | Send a request to the current leader. Nonblocking.
clientSend
  :: (Show (RaftClientSendError m v), RaftClientSend m v)
  => ClientRequest v
  -> RaftClientT s v m (Either (RaftClientSendError m v) ())
clientSend creq = do
  currLeader <- gets raftClientCurrentLeader
  case currLeader of
    NoLeader -> clientSendRandom creq
    CurrentLeader (LeaderId nid) -> do
      eRes <- raftClientSend nid creq
      case eRes of
        Left err -> clientSendRandom creq
        Right resp -> pure (Right resp)

-- | Send a request to a specific raft node, ignoring the current leader.
-- This function is used in testing.
clientSendTo
  :: RaftClientSend m v
  => NodeId
  -> ClientRequest v
  -> RaftClientT s v m (Either (RaftClientSendError m v) ())
clientSendTo nid creq = raftClientSend nid creq

-- | Send a request to a random node.
-- This function is used if there is no leader.
clientSendRandom
  :: RaftClientSend m v
  => ClientRequest v
  -> RaftClientT s v m (Either (RaftClientSendError m v) ())
clientSendRandom creq = do
  raftNodes <- gets raftClientRaftNodes
  randomGen <- gets raftClientRandomGen
  let (idx, newRandomGen) = randomR (0, length raftNodes - 1) randomGen
  case atMay (toList raftNodes) idx of
    Nothing -> panic "No raft nodes known by client"
    Just nid -> do
      modify $ \s -> s { raftClientRandomGen = newRandomGen }
      raftClientSend nid creq

--------------------------------------------------------------------------------

-- | Waits for a write response on the client socket
-- Warning: This function discards unexpected read and redirect responses
clientRecvWrite
  :: (RaftClientSend m v, RaftClientRecv m s)
  => RaftClientT s v m (Either (RaftClientError s v m) ClientWriteResp)
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
  :: (RaftClientSend m v, RaftClientRecv m s)
  => RaftClientT s v m (Either (RaftClientError s v m) (ClientReadResp s))
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
  :: RaftClientRecv m s
  => RaftClientT s v m (Either (RaftClientRecvError m s) (ClientResponse s))
clientRecv = do
  ecresp <- raftClientRecv
  case ecresp of
    Left err -> pure (Left err)
    Right cresp ->
      case cresp of
        ClientWriteResponse (ClientWriteResp _ (SerialNum n)) -> do
          SerialNum m <- gets raftClientSerialNum
          if m == n
          then do
            modify $ \s -> s
              { raftClientSerialNum = SerialNum (succ m) }
            pure (Right cresp)
          else
            -- Here, we ignore the response if we are receiving a response
            -- regarding a previously committed write request. This regularly
            -- happens when a write request is submitted, committed, and
            -- responded to, but the leader subsequently dies before letting all
            -- other nodes know that the write request has been committed.
            if n < m
            then clientRecv
            else do
              let errMsg = "Received invalid serial number response: Expected " <> show m <> " but got " <> show n
              panic $ errMsg
        ClientRedirectResponse (ClientRedirResp currLdr) -> do
          modify $ \s -> s
            { raftClientCurrentLeader = currLdr }
          pure (Right cresp)
        _ -> pure (Right cresp)

--------------------------------------------------------------------------------

clientAddNode :: Monad m => NodeId -> RaftClientT s v m ()
clientAddNode nid = modify $ \s ->
  s { raftClientRaftNodes = Set.insert nid (raftClientRaftNodes s) }

clientGetNodes :: Monad m => RaftClientT s v m (Set NodeId)
clientGetNodes = gets raftClientRaftNodes
