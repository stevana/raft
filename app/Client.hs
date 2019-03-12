{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DerivingStrategies #-}
module Client where
import Protolude

import qualified Data.List as L
import qualified Data.Set as Set
import Options.Applicative hiding (completer)
import Numeric.Natural
import System.Console.Repline

import Raft
import Raft.Log
import Raft.Client

import qualified Examples.Raft.Socket.Client as RS
import qualified Examples.Raft.Socket.Common as RS

import Store

--------------------
-- Client console --
--------------------

-- Clients interact with the nodes from a terminal:
-- Accepted operations are:
data ReplCmd
  -- | Add nodeId to the set of nodeIds that the client will communicate with
  = CmdAddNode NodeId
  -- | Return the node ids that the client is aware of
  | CmdGetNodes
  -- | Read leader state
  | CmdReadState
  -- | Read specific Entries
  | CmdRead Index
  -- | Read entries in interval
  | CmdReadInterval Index Index
  -- | Set variable to specific value
  | CmdSet Var Natural
  -- | Increment the value of a variable
  | CmdIncr Var
  | CmdHelp

replCmdParser :: Parser ReplCmd
replCmdParser = subparser $ mconcat
    [ command "addNode" $ info (CmdAddNode <$> strArgument (metavar "HOST")) $
        progDesc "Add nodeId to the set of nodeIds that the client will communicate with"
    , command "getNodes" $ info (pure CmdGetNodes) $
        progDesc "Return the node ids that the client is aware of"
    , command "readState" $ info (pure CmdReadState) $
        progDesc "Read leader state"
    , command "read" $ info (CmdRead <$> indexParser) $
        progDesc "Read specific entry"
    , command "readInterval" $ info (CmdReadInterval <$> indexParser <*> indexParser) $
        progDesc "Read entries in interval"
    , command "set" $ info (CmdSet <$> varParser <*> valueParser) $
        progDesc "Set variable to specific value"
    , command "incr" $ info (CmdIncr <$> varParser) $
        progDesc "Increment the value of a variable"
    , command "help" $ info (pure CmdHelp) $
        progDesc "Show this help text"
    ]
  where
    indexParser :: Parser Index
    indexParser = argument auto (metavar "IDX")

    valueParser :: Parser Natural
    valueParser = argument auto (metavar "VALUE")

    varParser :: Parser Var
    varParser = strArgument (metavar "VAR")

newtype ConsoleM a = ConsoleM
  { unConsoleM :: HaskelineT (RS.RaftSocketClientM Store StoreCmd) a
  } deriving newtype (Functor, Applicative, Monad, MonadIO)

liftRSCM = ConsoleM . lift

-- | Evaluate and handle each line user inputs
handleConsoleCmd :: [Char] -> ConsoleM ()
handleConsoleCmd input = do
  nids <- liftRSCM clientGetNodes
  let parser = info (replCmdParser <**> helper) fullDesc
  let parseResult = execParserPure defaultPrefs parser (L.words input)
  case parseResult of
    Failure f -> putStrLn $ fst (renderFailure f "")
    CompletionInvoked _ -> print "optparse-applicative completion for the repl should never be invoked"
    Success cmd -> case cmd of
      CmdAddNode nid -> liftRSCM $ clientAddNode (toS nid)
      CmdGetNodes -> print =<< liftRSCM clientGetNodes
      CmdReadState -> ifNodesAdded nids $
        handleResponse =<< liftRSCM (RS.socketClientRead ClientReadStateMachine)
      CmdRead n ->
        ifNodesAdded nids $
          handleResponse =<< liftRSCM (RS.socketClientRead (ClientReadEntries (ByIndex n)))
      CmdReadInterval low high ->
        ifNodesAdded nids $ do
          let byInterval = ByIndices $ IndexInterval (Just low) (Just high)
          handleResponse =<< liftRSCM (RS.socketClientRead (ClientReadEntries byInterval))
      CmdSet var val ->
        ifNodesAdded nids $
          handleResponse =<< liftRSCM (RS.socketClientWrite (Set var val))
      CmdIncr var ->
        ifNodesAdded nids $
          handleResponse =<< liftRSCM (RS.socketClientWrite (Incr var))
      CmdHelp -> do
        let fakeFail = parserFailure defaultPrefs parser ShowHelpText mempty
        putStrLn $ fst (renderFailure fakeFail "")

  where
    ifNodesAdded nids m
      | nids == Set.empty =
          putText "Please add some nodes to query first. Eg. `addNode localhost:3001`"
      | otherwise = m

    handleResponse :: Show a => Either Text a -> ConsoleM ()
    handleResponse res = do
      case res of
        Left err -> liftIO $ putText err
        Right resp -> liftIO $ putText (show resp)

addInitialNodes :: [ByteString] -> ConsoleM ()
addInitialNodes nodes =
  forM_ nodes $ \node ->
    liftRSCM $ clientAddNode (toS node)

clientRepl :: [ByteString] -> IO ()
clientRepl nodes = do
  let clientHost = "localhost"
  clientPort <- RS.getFreePort
  let clientId = ClientId $ RS.hostPortToNid (clientHost, show clientPort)
  clientRespChan <- RS.newClientRespChan
  RS.runRaftSocketClientM clientId mempty clientRespChan $ do
    evalRepl (pure ">>> ")
      (unConsoleM . handleConsoleCmd) [] Nothing (Word completer) (unConsoleM (addInitialNodes nodes))

-- Tab Completion: return a completion for partial words entered
completer :: Monad m => WordCompleter m
completer n = do
  let cmds =
        [ "addNode <host:port>"
        , "getNodes"
        , "incr <var>"
        , "set <var> <val>"
        , "readState"
        , "read <idx>"
        , "readInterval <low> <high>"
        ]
  return $ filter (isPrefixOf n) cmds
