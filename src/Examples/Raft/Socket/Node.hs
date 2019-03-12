{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Examples.Raft.Socket.Node where

import Protolude

import Control.Monad.Fail
import Control.Monad.Catch
import Control.Monad.Trans.Class
import Control.Concurrent.STM.TChan
import Control.Concurrent.STM.TVar
import Control.Concurrent.STM.TMVar

import qualified Data.Map as Map
import qualified Data.Serialize as S
import qualified Network.Simple.TCP as NS
import Network.Simple.TCP (HostName, ServiceName)

import Examples.Raft.Socket.Common

import Raft.Client
import Raft.Event
import Raft.Log
import Raft.Monad
import Raft.Persistent
import Raft.RPC
import Raft.StateMachine
import Raft.Types

--------------------------------------------------------------------------------
-- Network
--------------------------------------------------------------------------------

data ResponseSignal sm v
  = OkResponse (ClientResponse sm v)
    -- ^ we managed to write a valid response to the @TMVar@
  | DeadResponse
    -- ^ if we get overlapping requests coming in with the same client
    -- id, we "kill" one of them

data NodeSocketEnv sm v = NodeSocketEnv
  { nsMsgQueue :: TChan (RPCMessage v)
    -- ^ Queue of RPC messages to be processed by event handlers
  , nsClientReqQueue :: TChan (ClientRequest v)
    -- ^ Queue of client request messages to be processed by event handlers
  , nsClientReqResps :: TVar (Map ClientId (TMVar (ResponseSignal sm v)))
    -- ^ Map of variables to which responses to a request are written. N.B.:
    -- this assumes a client id uniquely identifies a request; A client
    -- will never send a request without having either 1) given up on the
    -- a previous request because of a timeout, or 2) received a response to
    -- each previous request issued.
  }

newtype RaftSocketT sm v m a = RaftSocketT { unRaftSocketT :: ReaderT (NodeSocketEnv sm v) m a }
  deriving (Functor, Applicative, Monad, MonadIO, MonadFail, MonadReader (NodeSocketEnv sm v), Alternative, MonadPlus, MonadTrans)

deriving instance MonadThrow m => MonadThrow (RaftSocketT sm v m)
deriving instance MonadCatch m => MonadCatch (RaftSocketT sm v m)
deriving instance MonadMask m => MonadMask (RaftSocketT sm v m)

--------------------
-- Raft Instances --
--------------------

instance (RaftStateMachinePure sm v, MonadMask m, MonadCatch m, MonadIO m, S.Serialize sm, S.Serialize v) => RaftSendClient (RaftSocketT sm v m) sm v where
  sendClient clientId msg = do
    NodeSocketEnv{..} <- ask
    mRespVar <- liftIO . atomically . fmap (Map.lookup clientId) . readTVar $ nsClientReqResps
    -- We write the response to the TMVar corresponding to the client
    -- id, such that @acceptConnections@ can send it back to the
    -- client.
    case mRespVar of
      Nothing -> liftIO $ putText "sendClient: response lookup failed"
      Just respVar -> liftIO . atomically . putTMVar respVar . OkResponse $ msg

instance (MonadIO m, S.Serialize v) => RaftRecvClient (RaftSocketT sm v m) v where
  type RaftRecvClientError (RaftSocketT sm v m) v = Text
  receiveClient = do
    cReq <- asks nsClientReqQueue
    fmap Right . liftIO . atomically $ readTChan cReq

instance (MonadCatch m, MonadMask m, MonadIO m, S.Serialize v, Show v) => RaftSendRPC (RaftSocketT sm v m) v where
  sendRPC nid msg = do
      eRes <- Control.Monad.Catch.try $
        NS.connect host port $ \(sock,_) -> do
          NS.send sock (S.encode $ RPCMessageEvent msg)
      case eRes of
        Left (err :: SomeException) -> putText ("Failed to send RPC: " <> show err)
        Right _ -> pure ()
    where
      (host, port) = nidToHostPort nid

instance (MonadIO m, Show v) => RaftRecvRPC (RaftSocketT sm v m) v where
  type RaftRecvRPCError (RaftSocketT sm v m) v = Text
  receiveRPC = do
    msgQueue <- asks nsMsgQueue
    fmap Right . liftIO . atomically $ readTChan msgQueue

runRaftSocketT :: MonadIO m => NodeSocketEnv sm v -> RaftSocketT sm v m a -> m a
runRaftSocketT nodeSocketEnv = flip runReaderT nodeSocketEnv . unRaftSocketT

acceptConnections
  :: forall sm v m.
     ( S.Serialize sm, S.Serialize v, S.Serialize (RaftStateMachinePureError sm v)
     , Show (RaftStateMachinePureError sm v)
     , MonadIO m
     )
  => HostName
  -> ServiceName
  -> RaftSocketT sm v m ()
acceptConnections host port = do
  NodeSocketEnv{..} <- ask
  NS.serve (NS.Host host) port $ \(sock, _) -> do
    mVal <- recvSerialized sock
    case mVal of
      Nothing -> putText "Socket was closed on the other end"
      Just (ClientRequestEvent req@(ClientRequest clientId _)) -> do
        -- Create and register TMVar where the response should be
        -- written to for this client id in the 'sendClient' impl
        respVar <- atomically $ do
                     newRespVar <- newEmptyTMVar
                     clientReqResps <- readTVar nsClientReqResps
                     -- If there's an outstanding request for this
                     -- client id, we send a signal that the request
                     -- is "dead". Given sufficiently unique client
                     -- ids, this case should never occur.
                     when (Map.member clientId clientReqResps) $
                       case Map.lookup clientId clientReqResps of
                         Nothing -> pure ()
                         Just reqVar -> putTMVar reqVar DeadResponse

                     writeTVar nsClientReqResps . Map.insert clientId newRespVar $ clientReqResps
                     pure newRespVar
        -- Register request to be handled by event handler
        atomically $ writeTChan nsClientReqQueue req
        -- Wait until response has been written to the TMVar by a 'sendClient'
        -- call and send the response to the client.
        respMsg <- atomically $ takeTMVar respVar
        case respMsg of
          OkResponse okResp -> NS.send sock (S.encode okResp)
          DeadResponse -> pure () -- ignored for now

        -- Remove response variable for the client id
        atomically $ do
          modifyTVar nsClientReqResps $ \_nsClientReqResps ->
            Map.delete clientId _nsClientReqResps
      Just (RPCMessageEvent msg) ->
        atomically $ writeTChan nsMsgQueue msg

--------------------------------------------------------------------------------
-- Inherited Instances
--------------------------------------------------------------------------------

instance (MonadIO m, RaftPersist m) => RaftPersist (RaftSocketT sm v m) where
  type RaftPersistError (RaftSocketT sm v m) = RaftPersistError m
  initializePersistentState = lift initializePersistentState
  writePersistentState ps = lift $ writePersistentState ps
  readPersistentState = lift readPersistentState

instance (MonadIO m, RaftInitLog m v) => RaftInitLog (RaftSocketT sm v m) v where
  type RaftInitLogError (RaftSocketT sm v m) = RaftInitLogError m
  initializeLog p = lift $ initializeLog p

instance RaftWriteLog m v => RaftWriteLog (RaftSocketT sm v m) v where
  type RaftWriteLogError (RaftSocketT sm v m) = RaftWriteLogError m
  writeLogEntries entries = lift $ writeLogEntries entries

instance RaftReadLog m v => RaftReadLog (RaftSocketT sm v m) v where
  type RaftReadLogError (RaftSocketT sm v m) = RaftReadLogError m
  readLogEntry idx = lift $ readLogEntry idx
  readLastLogEntry = lift readLastLogEntry

instance RaftDeleteLog m v => RaftDeleteLog (RaftSocketT sm v m) v where
  type RaftDeleteLogError (RaftSocketT sm v m) = RaftDeleteLogError m
  deleteLogEntriesFrom idx = lift $ deleteLogEntriesFrom idx

instance RaftStateMachine m sm v => RaftStateMachine (RaftSocketT sm v m) sm v where
  validateCmd = lift . validateCmd
  askRaftStateMachinePureCtx = lift askRaftStateMachinePureCtx

instance MonadRaftChan v m => MonadRaftChan v (RaftSocketT sm v m) where
  type RaftEventChan v (RaftSocketT sm v m) = RaftEventChan v m
  readRaftChan = lift . readRaftChan
  writeRaftChan chan = lift . writeRaftChan chan
  newRaftChan = lift (newRaftChan @v @m)

instance (MonadIO m, MonadRaftFork m) => MonadRaftFork (RaftSocketT sm v m) where
  type RaftThreadId (RaftSocketT sm v m) = RaftThreadId m
  raftFork r m = do
    persistFile <- ask
    lift $ raftFork r (runRaftSocketT persistFile m)
