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

import qualified Data.Serialize as S
import qualified Network.Simple.TCP as NS
import Network.Simple.TCP

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

data NodeSocketEnv v = NodeSocketEnv
  { nsMsgQueue :: TChan (RPCMessage v)
  , nsClientReqQueue :: TChan (ClientRequest v)
  }

newtype RaftSocketT v m a = RaftSocketT { unRaftSocketT :: ReaderT (NodeSocketEnv v) m a }
  deriving (Functor, Applicative, Monad, MonadIO, MonadFail, MonadReader (NodeSocketEnv v), Alternative, MonadPlus, MonadTrans)

deriving instance MonadThrow m => MonadThrow (RaftSocketT v m)
deriving instance MonadCatch m => MonadCatch (RaftSocketT v m)
deriving instance MonadMask m => MonadMask (RaftSocketT v m)

--------------------
-- Raft Instances --
--------------------

instance (MonadMask m, MonadCatch m, MonadIO m, S.Serialize sm, S.Serialize v) => RaftSendClient (RaftSocketT v m) sm v where
  sendClient clientId@(ClientId nid) msg = do
    let (cHost, cPort) = nidToHostPort (toS nid)
    eRes <- Control.Monad.Catch.try $
      connect cHost cPort $ \(cSock, _cSockAddr) ->
        send cSock (S.encode msg)
    case eRes of
      Left (err :: SomeException) -> putText ("Failed to send Client: " <> show err)
      Right _ -> pure ()

instance (MonadIO m, S.Serialize v) => RaftRecvClient (RaftSocketT v m) v where
  type RaftRecvClientError (RaftSocketT v m) v = Text
  receiveClient = do
    cReq <- asks nsClientReqQueue
    fmap Right . liftIO . atomically $ readTChan cReq

instance (MonadCatch m, MonadMask m, MonadIO m, S.Serialize v, Show v) => RaftSendRPC (RaftSocketT v m) v where
  sendRPC nid msg = do
      eRes <- Control.Monad.Catch.try $
        connect host port $ \(sock,_) -> do
          NS.send sock (S.encode $ RPCMessageEvent msg)
      case eRes of
        Left (err :: SomeException) -> putText ("Failed to send RPC: " <> show err)
        Right _ -> pure ()
    where
      (host, port) = nidToHostPort nid

instance (MonadIO m, Show v) => RaftRecvRPC (RaftSocketT v m) v where
  type RaftRecvRPCError (RaftSocketT v m) v = Text
  receiveRPC = do
    msgQueue <- asks nsMsgQueue
    fmap Right . liftIO . atomically $ readTChan msgQueue

runRaftSocketT :: MonadIO m => NodeSocketEnv v -> RaftSocketT v m a -> m a
runRaftSocketT nodeSocketEnv = flip runReaderT nodeSocketEnv . unRaftSocketT

acceptConnections
  :: forall v m. (S.Serialize v, MonadIO m)
  => HostName
  -> ServiceName
  -> RaftSocketT v m ()
acceptConnections host port = do
  socketEnv@NodeSocketEnv{..} <- ask
  serve (Host host) port $ \(sock, _) -> do
    mVal <- recvSerialized sock
    case mVal of
      Nothing -> putText "Socket was closed on the other end"
      Just val ->
        case val of
          ClientRequestEvent req ->
            atomically $ writeTChan nsClientReqQueue req
          RPCMessageEvent msg ->
            atomically $ writeTChan nsMsgQueue msg

newSock :: HostName -> ServiceName -> IO Socket
newSock host port =
  listen (Host host) port (pure . fst)

--------------------------------------------------------------------------------
-- Inherited Instances
--------------------------------------------------------------------------------

instance (MonadIO m, RaftPersist m) => RaftPersist (RaftSocketT v m) where
  type RaftPersistError (RaftSocketT v m) = RaftPersistError m
  initializePersistentState = lift initializePersistentState
  writePersistentState ps = lift $ writePersistentState ps
  readPersistentState = lift readPersistentState

instance (MonadIO m, RaftInitLog m v) => RaftInitLog (RaftSocketT v m) v where
  type RaftInitLogError (RaftSocketT v m) = RaftInitLogError m
  initializeLog p = lift $ initializeLog p

instance RaftWriteLog m v => RaftWriteLog (RaftSocketT v m) v where
  type RaftWriteLogError (RaftSocketT v m) = RaftWriteLogError m
  writeLogEntries entries = lift $ writeLogEntries entries

instance RaftReadLog m v => RaftReadLog (RaftSocketT v m) v where
  type RaftReadLogError (RaftSocketT v m) = RaftReadLogError m
  readLogEntry idx = lift $ readLogEntry idx
  readLastLogEntry = lift readLastLogEntry

instance RaftDeleteLog m v => RaftDeleteLog (RaftSocketT v m) v where
  type RaftDeleteLogError (RaftSocketT v m) = RaftDeleteLogError m
  deleteLogEntriesFrom idx = lift $ deleteLogEntriesFrom idx

instance RaftStateMachine m sm v => RaftStateMachine (RaftSocketT v m) sm v where
  validateCmd = lift . validateCmd
  askRaftStateMachinePureCtx = lift askRaftStateMachinePureCtx

instance MonadRaftChan v m => MonadRaftChan v (RaftSocketT v m) where
  type RaftEventChan v (RaftSocketT v m) = RaftEventChan v m
  readRaftChan = lift . readRaftChan
  writeRaftChan chan = lift . writeRaftChan chan
  newRaftChan = lift (newRaftChan @v @m)

instance (MonadIO m, MonadRaftFork m) => MonadRaftFork (RaftSocketT v m) where
  type RaftThreadId (RaftSocketT v m) = RaftThreadId m
  raftFork m = do
    persistFile <- ask
    lift $ raftFork (runRaftSocketT persistFile m)
