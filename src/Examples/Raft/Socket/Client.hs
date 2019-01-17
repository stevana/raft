{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}

module Examples.Raft.Socket.Client where

import Protolude hiding (STM, atomically, try, ThreadId, newTChan)

import Control.Concurrent.Classy hiding (catch)
import Control.Monad.Base
import Control.Monad.Catch
import Control.Monad.Trans.Class
import Control.Monad.Trans.Control

import qualified Data.Serialize as S
import qualified Network.Simple.TCP as N
import System.Random

import Raft.Client
import Raft.Event
import Raft.Types
import Examples.Raft.Socket.Common

import System.Timeout.Lifted (timeout)
import System.Console.Haskeline.MonadException (MonadException(..), RunIO(..))

newtype ClientRespChan s v
  = ClientRespChan { clientRespChan :: TChan (STM IO) (ClientResponse s v) }

newClientRespChan :: IO (ClientRespChan s v)
newClientRespChan = ClientRespChan <$> atomically newTChan

newtype RaftClientRespChanT s v m a
  = RaftClientRespChanT { unRaftClientRespChanT :: ReaderT (ClientRespChan s v) m a }
  deriving newtype (Functor, Applicative, Monad, MonadIO, MonadReader (ClientRespChan s v), Alternative, MonadPlus)

instance MonadTrans (RaftClientRespChanT s v) where
  lift = RaftClientRespChanT . lift

deriving instance MonadBase IO m => MonadBase IO (RaftClientRespChanT s v m)

instance MonadTransControl (RaftClientRespChanT s v) where
    type StT (RaftClientRespChanT s v) a = StT (ReaderT (ClientRespChan s v)) a
    liftWith = defaultLiftWith RaftClientRespChanT unRaftClientRespChanT
    restoreT = defaultRestoreT RaftClientRespChanT

instance MonadBaseControl IO m => MonadBaseControl IO (RaftClientRespChanT s v m) where
    type StM (RaftClientRespChanT s v m) a = ComposeSt (RaftClientRespChanT s v) m a
    liftBaseWith = defaultLiftBaseWith
    restoreM     = defaultRestoreM

deriving instance MonadSTM m => MonadSTM (RaftClientRespChanT s v m)
deriving instance MonadThrow m => MonadThrow (RaftClientRespChanT s v m)
deriving instance MonadCatch m => MonadCatch (RaftClientRespChanT s v m)
deriving instance MonadMask m => MonadMask (RaftClientRespChanT s v m)
deriving instance MonadConc m => MonadConc (RaftClientRespChanT s v m)

-- This annoying instance is because of the Haskeline library, letting us use a
-- custom monad transformer stack as the base monad of 'InputT'. IMO it should
-- be automatically derivable. Why does haskeline use a custom exception
-- monad... ?
instance MonadException m => MonadException (RaftClientRespChanT s v m) where
  controlIO f =
    RaftClientRespChanT $ ReaderT $ \r ->
      controlIO $ \(RunIO run) ->
        let run' = RunIO (fmap (RaftClientRespChanT . ReaderT . const) . run . flip runReaderT r . unRaftClientRespChanT)
         in fmap (flip runReaderT r . unRaftClientRespChanT) $ f run'

instance (S.Serialize v, MonadIO m) => RaftClientSend (RaftClientRespChanT s v m) v where
  type RaftClientSendError (RaftClientRespChanT s v m) v = Text
  raftClientSend nid creq = do
    let (host,port) = nidToHostPort nid
    mRes <-
      liftIO $ timeout 100000 $ try $ do
        -- Warning: blocks if socket is allocated by OS, even though the socket
        -- may have been closed by the running process
        N.connect host port $ \(sock, sockAddr) ->
          N.send sock (S.encode (ClientRequestEvent creq))
    let errPrefix = "Failed to send client request: "
    case mRes of
      Nothing -> pure (Left (errPrefix <> "'connect' timed out"))
      Just (Left (err :: SomeException)) -> pure $ Left (errPrefix <> show err)
      Just (Right _) -> pure (Right ())

instance (S.Serialize s, S.Serialize v, MonadIO m) => RaftClientRecv (RaftClientRespChanT s v m) s v where
  type RaftClientRecvError (RaftClientRespChanT s v m) s = Text
  raftClientRecv = do
    respChan <- asks clientRespChan
    fmap Right . liftIO . atomically $ readTChan respChan

--------------------------------------------------------------------------------

type RaftSocketClientM s v = RaftClientT s v (RaftClientRespChanT s v IO)

runRaftSocketClientM
  :: ClientId
  -> Set NodeId
  -> ClientRespChan s v
  -> RaftSocketClientM s v a
  -> IO a
runRaftSocketClientM cid nids respChan rscm = do
  raftClientState <- initRaftClientState nids <$> liftIO newStdGen
  let raftClientEnv = RaftClientEnv cid
  flip runReaderT respChan
    . unRaftClientRespChanT
    . runRaftClientT raftClientEnv raftClientState
    $ rscm

clientResponseServer
  :: forall s v m. (S.Serialize s, S.Serialize v, MonadIO m, MonadConc m)
  => N.HostName
  -> N.ServiceName
  -> RaftClientRespChanT s v m ()
clientResponseServer host port = do
  respChan <- asks clientRespChan
  N.serve (N.Host host) port $ \(sock, _) -> do
    mBytes <- N.recv sock (4 * 4096)
    case mBytes of
      Nothing -> putText "Socket was closed on the other end"
      Just bytes -> case S.decode bytes of
        Left err -> putText $ "Failed to decode message: " <> toS err
        Right cresp -> atomically $ writeTChan respChan cresp

-- | Send a client read request using the example socket interface of RaftSocketClientM
socketClientRead
  :: (S.Serialize s, S.Serialize v, Show s, Show v, Show (RaftClientError s v (RaftSocketClientM s v)))
  => ClientReadReq
  -> RaftSocketClientM s v (Either Text (ClientReadResp s v))
socketClientRead rr = first show <$> retryOnRedirect (clientReadTimeout 1000000 rr)

socketClientWrite
  :: (S.Serialize s, S.Serialize v, Show s, Show v, Show (RaftClientError s v (RaftSocketClientM s v)))
  => v
  -> RaftSocketClientM s v (Either Text ClientWriteResp)
socketClientWrite v = first show <$> retryOnRedirect (clientWriteTimeout 1000000 v)
