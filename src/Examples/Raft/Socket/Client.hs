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

newtype ClientRespChan s
  = ClientRespChan { clientRespChan :: TChan (STM IO) (ClientResponse s) }

newClientRespChan :: IO (ClientRespChan s)
newClientRespChan = ClientRespChan <$> atomically newTChan

newtype RaftClientRespChanT s m a
  = RaftClientRespChanT { unRaftClientRespChanT :: ReaderT (ClientRespChan s) m a }
  deriving newtype (Functor, Applicative, Monad, MonadIO, MonadReader (ClientRespChan s), Alternative, MonadPlus)

instance MonadTrans (RaftClientRespChanT s) where
  lift = RaftClientRespChanT . lift

deriving instance MonadBase IO m => MonadBase IO (RaftClientRespChanT s m)

instance MonadTransControl (RaftClientRespChanT s) where
    type StT (RaftClientRespChanT s) a = StT (ReaderT (ClientRespChan s)) a
    liftWith = defaultLiftWith RaftClientRespChanT unRaftClientRespChanT
    restoreT = defaultRestoreT RaftClientRespChanT

instance MonadBaseControl IO m => MonadBaseControl IO (RaftClientRespChanT s m) where
    type StM (RaftClientRespChanT s m) a = ComposeSt (RaftClientRespChanT s) m a
    liftBaseWith = defaultLiftBaseWith
    restoreM     = defaultRestoreM

deriving instance MonadSTM m => MonadSTM (RaftClientRespChanT s m)
deriving instance MonadThrow m => MonadThrow (RaftClientRespChanT s m)
deriving instance MonadCatch m => MonadCatch (RaftClientRespChanT s m)
deriving instance MonadMask m => MonadMask (RaftClientRespChanT s m)
deriving instance MonadConc m => MonadConc (RaftClientRespChanT s m)

-- This annoying instance is because of the Haskeline library, letting us use a
-- custom monad transformer stack as the base monad of 'InputT'. IMO it should
-- be automatically derivable. Why does haskeline use a custom exception
-- monad... ?
instance MonadException m => MonadException (RaftClientRespChanT s m) where
  controlIO f =
    RaftClientRespChanT $ ReaderT $ \r ->
      controlIO $ \(RunIO run) ->
        let run' = RunIO (fmap (RaftClientRespChanT . ReaderT . const) . run . flip runReaderT r . unRaftClientRespChanT)
         in fmap (flip runReaderT r . unRaftClientRespChanT) $ f run'

instance (S.Serialize v, MonadIO m) => RaftClientSend (RaftClientRespChanT s m) v where
  type RaftClientSendError (RaftClientRespChanT s m) v = Text
  raftClientSend nid creq = do
    let (host,port) = nidToHostPort nid
    mRes <-
      liftIO $ timeout 100000 $ try $ do
        -- Warning: blocks if socket is allocated by OS, even though the socket
        -- may have been closed by the running process
        N.connect host port $ \(sock, sockAddr) ->
          N.send sock (S.encode (ClientRequestEvent creq))
    let errPrefix = "Failed to send ClientWriteReq: "
    case mRes of
      Nothing -> pure (Left (errPrefix <> "'connect' timed out"))
      Just (Left (err :: SomeException)) -> pure $ Left (errPrefix <> show err)
      Just (Right _) -> pure (Right ())

instance (S.Serialize s, MonadIO m) => RaftClientRecv (RaftClientRespChanT s m) s where
  type RaftClientRecvError (RaftClientRespChanT s m) s = Text
  raftClientRecv = do
    respChan <- asks clientRespChan
    fmap Right . liftIO . atomically $ readTChan respChan

--------------------------------------------------------------------------------

type RaftSocketClientM s v = RaftClientT s v (RaftClientRespChanT s IO)

runRaftSocketClientM
  :: ClientId
  -> Set NodeId
  -> (ClientRespChan s)
  -> RaftSocketClientM s v a
  -> IO a
runRaftSocketClientM cid nids respChan rscm = do
  raftClientState <- initRaftClientState <$> liftIO newStdGen
  let raftClientEnv = RaftClientEnv cid
  flip runReaderT respChan
    . unRaftClientRespChanT
    . runRaftClientT raftClientEnv raftClientState
    $ rscm

clientResponseServer
  :: forall v m. (S.Serialize v, MonadIO m, MonadConc m)
  => N.HostName
  -> N.ServiceName
  -> RaftClientRespChanT v m ()
clientResponseServer host port = do
  respChan <- asks clientRespChan
  N.serve (N.Host host) port $ \(sock, _) -> do
    mBytes <- N.recv sock (4 * 4096)
    case mBytes of
      Nothing -> putText "Socket was closed on the other end"
      Just bytes -> case S.decode bytes of
        Left err -> putText $ "Failed to decode message: " <> toS err
        Right cresp -> atomically $ writeTChan respChan cresp

socketClientRead
  :: (S.Serialize s, S.Serialize v, Show s, Show (RaftClientError s v (RaftSocketClientM s v)))
  => RaftSocketClientM s v (Either Text (ClientReadResp s))
socketClientRead = first show <$> retryOnRedirect (clientReadTimeout 1000000)

socketClientWrite
  :: (S.Serialize s, S.Serialize v, Show s, Show (RaftClientError s v (RaftSocketClientM s v)))
  => v
  -> RaftSocketClientM s v (Either Text ClientWriteResp)
socketClientWrite v = first show <$> retryOnRedirect (clientWriteTimeout 1000000 v)
