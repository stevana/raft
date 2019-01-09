{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE RankNTypes #-}

module Raft.Types where

import Protolude

import Data.Serialize
import Numeric.Natural (Natural)

--------------------------------------------------------------------------------
-- NodeIds
--------------------------------------------------------------------------------

-- | Unique identifier of a Raft node
type NodeId = ByteString
type NodeIds = Set NodeId

-- | Unique identifier of a client
newtype ClientId = ClientId NodeId
  deriving stock (Show, Eq, Ord, Generic)
  deriving newtype Serialize

-- | Unique identifier of a leader
newtype LeaderId = LeaderId { unLeaderId :: NodeId }
  deriving stock (Show, Eq, Generic)
  deriving newtype Serialize

-- | Representation of the current leader in the cluster. The system is
-- considered to be unavailable if there is no leader
data CurrentLeader
  = CurrentLeader LeaderId
  | NoLeader
  deriving stock (Show, Eq, Generic)
  deriving anyclass (Serialize)

----------
-- Term --
----------

-- | Representation of monotonic election terms
newtype Term = Term Natural
  deriving stock (Generic)
  deriving newtype (Show, Eq, Ord, Enum, Serialize)

-- | Initial term. Terms start at 0
term0 :: Term
term0 = Term 0

incrTerm :: Term -> Term
incrTerm = succ

prevTerm :: Term -> Term
prevTerm (Term 0) = Term 0
prevTerm t = pred t

-----------
-- Index --
-----------

-- | Representation of monotonic indices
newtype Index = Index Natural
  deriving stock (Show, Generic)
  deriving newtype (Eq, Ord, Enum, Num, Integral, Real, Serialize)

-- | Initial index. Indeces start at 0
index0 :: Index
index0 = Index 0

incrIndex :: Index -> Index
incrIndex = succ

-- | Decrement index.
-- If the given index is 0, return the given index
decrIndexWithDefault0 :: Index -> Index
decrIndexWithDefault0 (Index 0) = index0
decrIndexWithDefault0 i = pred i

-----------------------------------
-- Client Request Serial Numbers --
-----------------------------------

newtype SerialNum = SerialNum Natural
  deriving stock (Show, Generic)
  deriving newtype (Read, Eq, Ord, Enum, Num, Serialize)
