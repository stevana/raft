{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeFamilyDependencies #-}

module Raft.StateMachine (
  RaftStateMachinePure(..),
  RaftStateMachinePureError(..),
  RaftStateMachine(..),

  applyLogEntry
) where

import Protolude

import Raft.Log (Entry(..), EntryValue(..))

--------------------------------------------------------------------------------
-- State Machine
--------------------------------------------------------------------------------

-- | Interface to handle commands in the underlying state machine. Functional
-- dependency permitting only a single state machine command to be defined to
-- update the state machine.
class RaftStateMachinePure sm v | sm -> v where
  data RaftStateMachinePureError sm v
  -- This type family dependency is to make the 'RaftStateMachinePureCtx` type
  -- family injective; i.e. to allow GHC to infer the state machine and command
  -- types from values of type 'RaftStateMachinePureCtx sm v'.
  type RaftStateMachinePureCtx sm v = ctx | ctx -> sm v
  rsmTransition
    :: RaftStateMachinePureCtx sm v
    -> sm
    -> v
    -> Either (RaftStateMachinePureError sm v) sm

class (Monad m, RaftStateMachinePure sm v) => RaftStateMachine m sm v where
  validateCmd :: v -> m (Either (RaftStateMachinePureError sm v) ())
  askRaftStateMachinePureCtx :: m (RaftStateMachinePureCtx sm v)

applyLogEntry
  :: RaftStateMachine m sm v
  => sm
  -> Entry v
  -> m (Either (RaftStateMachinePureError sm v) sm)
applyLogEntry sm e  =
  case entryValue e of
    NoValue -> pure (Right sm)
    EntryValue v -> do
      res <- validateCmd v
      case res of
        Left err -> pure (Left err)
        Right () -> do
          ctx <- askRaftStateMachinePureCtx
          pure (rsmTransition ctx sm v)
