{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeFamilyDependencies #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE TypeApplications #-}

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
  preprocessCmd :: v -> m v
  -- ^ Some state machines need the leader to preprocess commands issued by
  -- client; e.g. attaching a timestamp before creating the log entry
  askRaftStateMachinePureCtx :: m (RaftStateMachinePureCtx sm v)
  -- ^ Query the 'RaftStateMachinePureCtx' value from the monadic context

  -- Preprocessing a command is not always needed, therefore the default
  -- implementation is to just return the command unchanged.
  default preprocessCmd :: v -> m v
  preprocessCmd = return

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
  :: forall sm cmd m. RaftStateMachine m sm cmd
  => EntryValidation
  -> sm
  -> cmd
  -> m (Either (RaftStateMachinePureError sm cmd) sm)
applyLogCmd validation sm cmd = do
  processedCmd <- preprocessCmd @m @sm cmd
  monadicValidationRes <-
    case validation of
      NoMonadicValidation -> pure (Right ())
      MonadicValidation -> validateCmd processedCmd
  case monadicValidationRes of
    Left err -> pure (Left err)
    Right () -> do
      ctx <- askRaftStateMachinePureCtx
      pure (rsmTransition ctx sm processedCmd)
