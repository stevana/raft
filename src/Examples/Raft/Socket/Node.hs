{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Examples.Raft.Socket.Node where

import Protolude hiding
  ( MVar, putMVar, takeMVar, newMVar, newEmptyMVar, readMVar
  , atomically, STM(..), Chan, newTVar, readTVar, writeTVar
  , newChan, writeChan, readChan
  , threadDelay, killThread, TVar(..)
  , catch, handle, takeWhile, takeWhile1, (<|>)
  )

import Control.Concurrent.Classy hiding (catch, ThreadId)
import Control.Monad.Fail
import Control.Monad.Catch
import Control.Monad.Trans.Class

import qualified Data.Serialize as S
import qualified Network.Simple.TCP as NS
import Network.Simple.TCP

import Examples.Raft.Socket.Common

import Raft

--------------------------------------------------------------------------------
-- Network
--------------------------------------------------------------------------------

data NodeSocketEnv v = NodeSocketEnv
  { nsMsgQueue :: TChan (STM IO) (RPCMessage v)
  , nsClientReqQueue :: TChan (STM IO) (ClientRequest v)
  }

newtype RaftSocketT v m a = RaftSocketT { unRaftSocketT :: ReaderT (NodeSocketEnv v) m a }
  deriving (Functor, Applicative, Monad, MonadIO, MonadFail, MonadReader (NodeSocketEnv v), Alternative, MonadPlus, MonadTrans)

deriving instance MonadThrow m => MonadThrow (RaftSocketT v m)
deriving instance MonadCatch m => MonadCatch (RaftSocketT v m)
deriving instance MonadMask m => MonadMask (RaftSocketT v m)
deriving instance MonadConc m => MonadConc (RaftSocketT v m)

--------------------
-- Raft Instances --
--------------------

instance (MonadIO m, MonadConc m, S.Serialize sm, S.Serialize v) => RaftSendClient (RaftSocketT v m) sm v where
  sendClient clientId@(ClientId nid) msg = do
    let (cHost, cPort) = nidToHostPort (toS nid)
    eRes <- Control.Monad.Catch.try $
      connect cHost cPort $ \(cSock, _cSockAddr) ->
        send cSock (S.encode msg)
    case eRes of
      Left (err :: SomeException) -> putText ("Failed to send Client: " <> show err)
      Right _ -> pure ()

instance (MonadIO m, MonadConc m, S.Serialize v) => RaftRecvClient (RaftSocketT v m) v where
  type RaftRecvClientError (RaftSocketT v m) v = Text
  receiveClient = do
    cReq <- asks nsClientReqQueue
    fmap Right . liftIO . atomically $ readTChan cReq

instance (MonadIO m, MonadConc m, S.Serialize v, Show v) => RaftSendRPC (RaftSocketT v m) v where
  sendRPC nid msg = do
      eRes <- Control.Monad.Catch.try $
        connect host port $ \(sock,_) -> do
          NS.send sock (S.encode $ RPCMessageEvent msg)
      case eRes of
        Left (err :: SomeException) -> putText ("Failed to send RPC: " <> show err)
        Right _ -> pure ()
    where
      (host, port) = nidToHostPort nid

instance (MonadIO m, MonadConc m, Show v) => RaftRecvRPC (RaftSocketT v m) v where
  type RaftRecvRPCError (RaftSocketT v m) v = Text
  receiveRPC = do
    msgQueue <- asks nsMsgQueue
    fmap Right . liftIO . atomically $ readTChan msgQueue

runRaftSocketT :: (MonadIO m, MonadConc m) => NodeSocketEnv v -> RaftSocketT v m a -> m a
runRaftSocketT nodeSocketEnv = flip runReaderT nodeSocketEnv . unRaftSocketT

acceptConnections
  :: forall v m. (S.Serialize v, MonadIO m, MonadConc m)
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
newSock host port = do
  (sock, _) <- bindSock (Host host) port
  listenSock sock 2048
  pure sock
