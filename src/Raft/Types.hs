{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE StrictData #-}

module Raft.Types where

import Protolude

import Data.Serialize
import Numeric.Natural (Natural)

import Database.PostgreSQL.Simple.ToField
import Database.PostgreSQL.Simple.FromField

--------------------------------------------------------------------------------
-- NodeIds
--------------------------------------------------------------------------------

-- | Unique identifier of a Raft node
type NodeId = ByteString
type NodeIds = Set NodeId

-- | Unique identifier of a client
newtype ClientId = ClientId NodeId
  deriving stock (Show, Read, Eq, Ord, Generic)
  deriving newtype Serialize

-- | Unique identifier of a leader
newtype LeaderId = LeaderId { unLeaderId :: NodeId }
  deriving stock (Show, Read, Eq, Generic)
  deriving newtype Serialize

-- | Representation of the current leader in the cluster. The system is
-- considered to be unavailable if there is no leader
data CurrentLeader
  = CurrentLeader LeaderId
  | NoLeader
  deriving stock (Show, Eq, Generic)
  deriving anyclass (Serialize)

----------------
-- Node State --
----------------

data Mode
  = Follower
  | Candidate
  | Leader
  deriving (Show, Read)

----------
-- Term --
----------

-- | Representation of monotonic election terms
newtype Term = Term Natural
  deriving stock (Generic)
  deriving newtype (Show, Eq, Ord, Enum, Serialize)

instance ToField Term where
  toField (Term n) = toField (fromIntegral n :: Int)

instance FromField Term where
  fromField f mdata =
    case (intToNatural <=< readEither) . toS <$> mdata of
      Nothing -> returnError UnexpectedNull f ""
      Just (Left err) -> returnError ConversionFailed f err
      Just (Right nat) -> return (Term nat)

intToNatural :: Int -> Either [Char] Natural
intToNatural n
  | n < 0 = Left "Natural numbers must be >= 0"
  | otherwise = Right (fromIntegral n)

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
  deriving stock (Show, Read, Generic)
  deriving newtype (Eq, Ord, Enum, Num, Integral, Real, Serialize)

instance ToField Index where
  toField (Index n) = toField (fromIntegral n :: Int)

instance FromField Index where
  fromField f mdata =
    case (intToNatural <=< readEither) . toS <$> mdata of
      Nothing -> returnError UnexpectedNull f ""
      Just (Left err) -> returnError ConversionFailed f err
      Just (Right nat) -> return (Index nat)

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
  deriving stock (Show, Read, Generic)
  deriving newtype (Eq, Ord, Enum, Num, Serialize)
