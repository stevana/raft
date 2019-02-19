{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeFamilyDependencies #-}

module Raft.StateMachine (
  RaftStateMachinePure(..),
  RaftStateMachinePureError(..),
  RaftStateMachine(..),

  EntryValidation(..),
  applyLogEntry,
  applyLogCmd
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
  -- ^ Expensive validation using global state not cacheable in
  -- 'RaftStateMachinePureCtx'
  askRaftStateMachinePureCtx :: m (RaftStateMachinePureCtx sm v)
  -- ^ Query the 'RaftStateMachinePureCtx' value from the monadic context

data EntryValidation
  = NoMonadicValidation
  | MonadicValidation

-- | Apply a log entry to the supplied state machine, allowing the user to
-- specify whether or not to monadically validate the command in addition to
-- the pure validation logic.
--
-- This function first unwraps the log entry to see if it is a no-op or contains
-- an actual state machine command to apply.
applyLogEntry
  :: RaftStateMachine m sm v
  => EntryValidation
  -> sm
  -> Entry v
  -> m (Either (RaftStateMachinePureError sm v) sm)
applyLogEntry validation sm entry =
  case entryValue entry of
    NoValue -> pure (Right sm)
    EntryValue cmd -> applyLogCmd validation sm cmd

-- | Apply a state machine command to the supplied state machine, allowing the
-- user to specify whether or not to monadically validate the command in
-- addition to the pure validation logic.
applyLogCmd
  :: RaftStateMachine m sm cmd
  => EntryValidation
  -> sm
  -> cmd
  -> m (Either (RaftStateMachinePureError sm cmd) sm)
applyLogCmd validation sm cmd = do
  monadicValidationRes <-
    case validation of
      NoMonadicValidation -> pure (Right ())
      MonadicValidation -> validateCmd cmd
  case monadicValidationRes of
    Left err -> pure (Left err)
    Right () -> do
      ctx <- askRaftStateMachinePureCtx
      pure (rsmTransition ctx sm cmd)
