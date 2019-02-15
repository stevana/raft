{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}
module RaftExampleT where

import Protolude hiding (STM)
import Control.Monad.Fail
import Control.Monad.Trans.Class
import Control.Monad.Catch

import Raft

import Raft.Monad
import Store

--------------------
-- Raft instances --
--------------------

newtype RaftExampleT m a = RaftExampleT { unRaftExampleT :: m a }
  deriving newtype (Functor, Applicative, Monad, MonadIO, MonadFail, MonadMask, MonadCatch, MonadThrow)

instance MonadTrans RaftExampleT where
  lift = RaftExampleT

instance (Monad m, RaftStateMachinePure Store StoreCmd) => RaftStateMachine (RaftExampleT m) Store StoreCmd where
  validateCmd _ = pure (Right ())
  askRaftStateMachinePureCtx = pure ()

instance MonadRaftChan v m => MonadRaftChan v (RaftExampleT m) where
  type RaftEventChan v (RaftExampleT m) = RaftEventChan v m
  readRaftChan = lift . readRaftChan
  writeRaftChan chan = lift . writeRaftChan chan
  newRaftChan = lift (newRaftChan @v @m)

instance (MonadIO m, MonadRaftFork m) => MonadRaftFork (RaftExampleT m) where
  type RaftThreadId (RaftExampleT m) = RaftThreadId m
  raftFork r m = lift $ raftFork r (runRaftExampleT m)

runRaftExampleT :: RaftExampleT m a -> m a
runRaftExampleT = unRaftExampleT
