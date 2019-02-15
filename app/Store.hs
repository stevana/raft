{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Store where

import Protolude

import qualified Data.Map as Map
import Data.Serialize (Serialize)

import Numeric.Natural
import Raft

------------------------------
-- State Machine & Commands --
------------------------------

-- State machine with two basic operations: set a variable to a value and
-- increment value
--
type Var = ByteString

data StoreCmd
  = Set Var Natural
  | Incr Var
  deriving stock (Show, Generic)
  deriving anyclass Serialize

type Store = Map Var Natural

instance RaftStateMachinePure Store StoreCmd where
  data RaftStateMachinePureError Store StoreCmd = StoreError Text deriving (Show)
  type RaftStateMachinePureCtx Store StoreCmd = ()

  rsmTransition _ store cmd =
    Right $ case cmd of
      Set x n -> Map.insert x n store
      Incr x -> Map.adjust succ x store
