{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RecordWildCards #-}

module Raft.Logging where

import Protolude

import Control.Monad.Trans.Class (MonadTrans)
import Control.Monad.State (modify')

import Data.Time
import Data.Time.Clock.System

import Raft.NodeState
import Raft.Types

-- | Representation of the logs' context
data LogCtx m
  = LogCtx
    { logCtxDest :: LogDest m
    , logCtxSeverity :: Severity
    }
  | NoLogs

-- | Representation of the logs' destination
data LogDest m
  = LogWith (MonadIO m => Severity -> Text -> m ())
  | LogFile FilePath
  | LogStdout

-- | Representation of the severity of the logs
data Severity
  = Debug
  | Info
  | Critical
  deriving (Show, Eq, Ord)

data LogMsg = LogMsg
  { mTime :: Maybe SystemTime
  , severity :: Severity
  , logMsgData :: LogMsgData
  }

data LogMsgData = LogMsgData
  { logMsgNodeId :: NodeId
  , logMsgNodeState :: Mode
  , logMsg :: Text
  } deriving (Show)

logMsgToText :: LogMsg -> Text
logMsgToText (LogMsg mt s d) =
    maybe "" timeToText mt <> "(" <> show s <> ")" <> " " <> logMsgDataToText d
  where
    timeToText :: SystemTime -> Text
    timeToText sysTime = "[" <> toS (timeToText' (systemToUTCTime sysTime)) <> ":" <> show ms <> "]"
      where
        ms = (systemNanoseconds sysTime) `div` 1000000

    timeToText' = formatTime defaultTimeLocale (iso8601DateFormat (Just "%H:%M:%S"))

logMsgDataToText :: LogMsgData -> Text
logMsgDataToText LogMsgData{..} =
  "<" <> toS logMsgNodeId <> " | " <> show logMsgNodeState <> ">: " <> logMsg

class Monad m => RaftLogger v m | m -> v where
  loggerCtx :: m (NodeId, RaftNodeState v)

mkLogMsgData :: RaftLogger v m => Text -> m (LogMsgData)
mkLogMsgData msg = do
  (nid, nodeState) <- loggerCtx
  let mode = nodeMode nodeState
  pure $ LogMsgData nid mode msg

instance RaftLogger v m => RaftLogger v (RaftLoggerT v m) where
  loggerCtx = lift loggerCtx

--------------------------------------------------------------------------------
-- Logging with IO
--------------------------------------------------------------------------------

logToDest :: MonadIO m => LogCtx m -> LogMsg -> m ()
logToDest LogCtx{..} logMsg = do
  let msgSeverity = severity logMsg
  case logCtxDest of
    LogWith f -> if msgSeverity >= logCtxSeverity
                  then f msgSeverity (logMsgToText logMsg)
                  else pure ()
    LogStdout -> if msgSeverity >= logCtxSeverity
                    then liftIO $ putText (logMsgToText logMsg)
                    else pure ()
    LogFile fp -> if msgSeverity >= logCtxSeverity
                    then liftIO $ appendFile fp (logMsgToText logMsg <> "\n")
                    else pure ()
logToDest NoLogs _ = pure ()

logToStdout :: MonadIO m => Severity -> LogMsg -> m ()
logToStdout s = logToDest $ LogCtx LogStdout s

logToFile :: MonadIO m => FilePath -> Severity -> LogMsg -> m ()
logToFile fp s = logToDest $ LogCtx (LogFile fp) s

logWithSeverityIO :: forall m v. (RaftLogger v m, MonadIO m) => Severity -> LogCtx m -> Text -> m ()
logWithSeverityIO s logCtx msg = do
  logMsgData <- mkLogMsgData msg
  sysTime <- liftIO getSystemTime
  let logMsg = LogMsg (Just sysTime) s logMsgData
  logToDest logCtx logMsg

logInfoIO :: (RaftLogger v m, MonadIO m) => LogCtx m -> Text -> m ()
logInfoIO = logWithSeverityIO Info

logDebugIO :: (RaftLogger v m, MonadIO m) => LogCtx m -> Text -> m ()
logDebugIO = logWithSeverityIO Debug

logCriticalIO :: (RaftLogger v m, MonadIO m) => LogCtx m -> Text -> m ()
logCriticalIO = logWithSeverityIO Critical

--------------------------------------------------------------------------------
-- Pure Logging
--------------------------------------------------------------------------------

newtype RaftLoggerT v m a = RaftLoggerT {
    unRaftLoggerT :: StateT [LogMsg] m a
  } deriving (Functor, Applicative, Monad, MonadState [LogMsg], MonadTrans)

runRaftLoggerT
  :: Monad m
  => RaftLoggerT v m a -- ^ The computation from which to extract the logs
  -> m (a, [LogMsg])
runRaftLoggerT = flip runStateT [] . unRaftLoggerT

type RaftLoggerM v = RaftLoggerT v Identity

runRaftLoggerM
  :: RaftLoggerM v a
  -> (a, [LogMsg])
runRaftLoggerM = runIdentity . runRaftLoggerT

logWithSeverity :: RaftLogger v m => Severity -> Text -> RaftLoggerT v m ()
logWithSeverity s txt = do
  !logMsgData <- mkLogMsgData txt
  let !logMsg = LogMsg Nothing s logMsgData
  modify' (++ [logMsg])

logInfo :: RaftLogger v m => Text -> RaftLoggerT v m ()
logInfo = logWithSeverity Info

logDebug :: RaftLogger v m => Text -> RaftLoggerT v m ()
logDebug = logWithSeverity Debug

logCritical :: RaftLogger v m => Text -> RaftLoggerT v m ()
logCritical = logWithSeverity Critical

--------------------------------------------------------------------------------
-- Panic after logging
--------------------------------------------------------------------------------

logAndPanic :: RaftLogger v m => Text -> m a
logAndPanic msg = do
  runRaftLoggerT $ logCritical msg
  panic ("logAndPanic: " <> msg)

logAndPanicIO :: (RaftLogger v m, MonadIO m) => LogCtx m -> Text -> m a
logAndPanicIO logCtx msg = do
  logCriticalIO logCtx msg
  panic ("logAndPanicIO: " <> msg)
