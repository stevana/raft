{-# LANGUAGE RecordWildCards #-}
module Main where
import Protolude hiding (option)

import Options.Applicative

import qualified Data.ByteString.Char8 as BS
import Data.Semigroup ((<>))
import Data.List

import Database.PostgreSQL.Simple.URL

import Client
import Node

import Raft.Config

data Command
  = Client
  | Node
    { _hostname :: ByteString
    , _storageState :: StorageState
    , _storageType :: LogStorage
    }

data Options = Options
  { _command :: Command
  , _verbose :: Bool
  , _nodes :: [ByteString]
  }

verbose :: Parser Bool
verbose = switch (long "verbose" <> short 'v' <> help "Verbose logging")

storageState :: Parser StorageState
storageState = mkStorageType <$> switch (long "reset" <> short 'r' <> help "storage")
  where
    mkStorageType b = if b then New else Existing

hostname :: Parser ByteString
hostname = argument str (metavar "HOST")

nodes :: Parser [ByteString]
nodes = fmap (fmap BS.pack . words) <$> strOption $ mconcat
  [ long "nodes"
  , help "Specify nodes"
  , showDefault
  , value "localhost:3000"
  , metavar "HOST1 HOST2 HOSTN.."
  ]

logStorageReader :: ReadM LogStorage
logStorageReader = eitherReader $ \url ->
  case parseDatabaseUrl url of
    Nothing -> Left "Failed to parse PostgreSQL URI"
    Just s -> Right $ PostgreSQL s

-- | Defaults to FileStore
-- ( no specifics required for FileStore backend yet? )
logStorage :: Parser LogStorage
logStorage =
  option logStorageReader
    (long "postgres"  <> metavar "URI" <> value FileStore)

opts :: Parser Options
opts = Options <$> commands <*> verbose <*> nodes
  where
    commands = subparser (client <> node)
    client = command "client" (info (pure Client) (progDesc ""))
    node = command
      "node"
        (info (Node <$> hostname <*> storageState <*> logStorage) (progDesc ""))

main :: IO ()
main = app =<< execParser parseArgs
 where
  parseArgs = info
    (opts <**> helper)
    (fullDesc <> progDesc "Raft Example Client" <> header "Raft Example Client")

app :: Options -> IO ()
app Options {..} = case _command of
  Client -> clientRepl _nodes
  Node {..} ->
    nodeMain _storageState _storageType _hostname _nodes
