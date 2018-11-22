{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveGeneric #-}
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
  deriving (Show, Eq, Ord, Generic, Serialize)

-- | Unique identifier of a leader
newtype LeaderId = LeaderId { unLeaderId :: NodeId }
  deriving (Show, Eq, Generic, Serialize)

----------
-- Term --
----------

-- | Representation of monotonic election terms
newtype Term = Term Natural
  deriving (Show, Eq, Ord, Enum, Generic, Serialize)

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
  deriving (Show, Eq, Ord, Enum, Num, Integral, Real, Generic, Serialize)

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
