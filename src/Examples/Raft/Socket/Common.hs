module Examples.Raft.Socket.Common where

import Protolude

import qualified Data.ByteString as BS
import qualified Data.Serialize as S
import qualified Data.Serialize as Get
import qualified Data.Word8 as W8

import qualified Network.Simple.TCP as N
import qualified Network.Socket as NS

import Raft.Types

-- | Convert a host and a port to a valid NodeId
hostPortToNid :: (N.HostName, N.ServiceName) -> NodeId
hostPortToNid = toS . hostPortToNidBS

hostPortToNidBS :: (N.HostName, N.ServiceName) -> ByteString
hostPortToNidBS (host, port) = toS $ host ++ ":" ++ toS port

-- | Retrieve the host and port from a valid NodeId
nidToHostPort :: NodeId -> (N.HostName, N.ServiceName)
nidToHostPort bs =
  case BS.split W8._colon bs of
    [host,port] -> (toS host, toS port)
    _ -> panic "nidToHostPort: invalid node id"

-- | Get a free port number.
getFreePort :: IO N.ServiceName
getFreePort = do
  sock <- NS.socket NS.AF_INET NS.Stream NS.defaultProtocol
  NS.bind sock (NS.SockAddrInet NS.aNY_PORT NS.iNADDR_ANY)
  port <- NS.socketPort sock
  NS.close sock
  pure $ show port

-- | Receive bytes on a socket until an entire message can be decoded.
-- This function fixes the deserialization of the bytes sent on the socket to
-- the implementation of the Serialize typeclass.
recvSerialized :: S.Serialize a => N.Socket -> IO (Maybe a)
recvSerialized sock = do
  result <- go $ Get.runGetPartial S.get
  case result of
    Get.Fail {} -> pure Nothing
    Get.Done val _ -> pure . pure $ val
    Get.Partial {} -> pure Nothing
  where
    go getPartial = do
      bytes <- fromMaybe BS.empty <$> N.recv sock 4096
      case getPartial bytes  of
        Get.Partial getNextPartial -> go getNextPartial
        x -> pure x
