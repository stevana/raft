

module Raft.Config where

import Protolude

import Numeric.Natural (Natural)
import Network.Socket

import Raft.Types

import System.Random (randomIO)

-- | Configuration of a node in the cluster
data RaftNodeConfig = RaftNodeConfig
  { raftConfigNodeId :: NodeId -- ^ Node id of the running node
  , raftConfigNodeIds :: NodeIds -- ^ Set of all other node ids in the cluster
  , raftConfigElectionTimeout :: (Natural, Natural) -- ^ Range of times an election timeout can take
  , raftConfigHeartbeatTimeout :: Natural -- ^ Heartbeat timeout timer
  , raftConfigStorageState :: StorageState -- ^ Create a fresh DB or read from existing
  } deriving (Show)

data StorageState = New | Existing
  deriving Show

data OptionalRaftNodeConfig = OptionalRaftNodeConfig
  { raftConfigMetricsPort :: Maybe PortNumber
  , raftConfigTimerSeed :: Maybe Int
  } deriving (Show)

defaultOptionalRaftNodeConfig :: OptionalRaftNodeConfig
defaultOptionalRaftNodeConfig =
  OptionalRaftNodeConfig Nothing Nothing

data ConfigError
  = InvalidMetricsPort
  | NoFreePortAvailable
  deriving (Show)

resolveMetricsPort
  :: Maybe PortNumber
  -> IO (Maybe PortNumber)
resolveMetricsPort mPort =
  case mPort of
    Nothing -> pure Nothing
    Just port -> do
      eMetricsPort <- resolveMetricsPortE port
      case eMetricsPort of
        Left err -> panic ("Error in raft node config: " <> show err)
        Right port -> pure (Just port)

-- | If the user specifies a port to fork the EKG server on, make sure the port
-- is open and return the valid port number. If the user does not specify a port
-- to run the monitoring server on, return Nothing.
resolveMetricsPortE
  :: PortNumber
  -> IO (Either ConfigError PortNumber)
resolveMetricsPortE port
  | port > 0 && port <= 65535 = do
      let hints = defaultHints { addrFlags = [AI_NUMERICHOST, AI_NUMERICSERV], addrSocketType = Stream }
      addrs <- getAddrInfo (Just hints) (Just "127.0.0.1") (Just (show port))
      case addrs of
        [] -> pure (Left NoFreePortAvailable)
        addr:_ -> do
          sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
          Network.Socket.bind sock (addrAddress addr)
          freePort <- socketPort sock
          close sock
          pure (Right freePort)
  | otherwise = pure (Left InvalidMetricsPort)

resolveTimerSeed
  :: Maybe Int
  -> IO Int
resolveTimerSeed mSeed = do
  case mSeed of
    Just seed -> pure seed
    Nothing -> randomIO
