{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE FlexibleInstances  #-}
{-# LANGUAGE PolyKinds          #-}
{-# LANGUAGE RecordWildCards    #-}
{-# LANGUAGE StandaloneDeriving #-}

module QuickCheckStateMachine where

import           Control.Concurrent            (threadDelay)
import           Control.Exception             (bracket)
import           Control.Monad                 (when)
import           Control.Monad.IO.Class        (liftIO)
import           Data.Bifunctor                (bimap)
import           Data.Char                     (isDigit)
import           Data.List                     (isInfixOf, (\\))
import           Data.Maybe                    (fromMaybe, isNothing, isJust)
import qualified Data.Set                      as Set
import           Data.TreeDiff                 (ToExpr)
import           GHC.Generics                  (Generic, Generic1)
import           Prelude
import           System.Directory              (removePathForcibly)
import           System.IO                     (BufferMode (NoBuffering),
                                                Handle, IOMode (WriteMode),
                                                hClose, hFlush, hGetContents,
                                                hGetLine, hPrint, hPutStrLn,
                                                hPutStrLn, hSetBuffering,
                                                openFile, stdout)
import           System.Process                (ProcessHandle, StdStream (CreatePipe, Inherit, UseHandle),
                                                callCommand, createProcess,
                                                createProcess_, getPid,
                                                getProcessExitCode, proc,
                                                spawnProcess, std_err, std_in,
                                                std_out, terminateProcess,
                                                waitForProcess)
import           System.Timeout                (timeout)
import           Test.QuickCheck
import           Test.QuickCheck.Monadic       (monadicIO)
import           Test.StateMachine
import           Test.StateMachine.Types       (Command (..), Commands (..),
                                                Reference (..), Symbolic (..),
                                                Var (..))
import qualified Test.StateMachine.Types.Rank2 as Rank2
import           Text.Read                     (readEither)

------------------------------------------------------------------------

type Port = Int

data Action (r :: * -> *)
  = SpawnNode Port
  | KillNode (Port, Reference (Opaque ProcessHandle) r)
  | Set Integer
  | Read
  | Incr
  | BreakConnection (Port, Reference (Opaque ProcessHandle) r)
  | FixConnection   (Port, Reference (Opaque ProcessHandle) r)
  deriving (Show, Generic1, Rank2.Functor, Rank2.Foldable, Rank2.Traversable)

data Response (r :: * -> *)
  = SpawnedNode (Reference (Opaque ProcessHandle) r)
  | SpawnFailed Port
  | Ack
  | Value (Either String Integer)
  deriving (Show, Generic1, Rank2.Foldable)

data Model (r :: * -> *) = Model
  { nodes    :: [(Port, Reference (Opaque ProcessHandle) r)]
  , value    :: Maybe Integer
  , isolated :: Maybe (Port, Reference (Opaque ProcessHandle) r)
  }
  deriving (Show, Generic)

deriving instance ToExpr (Model Concrete)

initModel :: Model r
initModel = Model [] Nothing Nothing

transition :: Model r -> Action r -> Response r -> Model r
transition Model {..} act resp = case (act, resp) of
  (SpawnNode port, SpawnedNode ph) -> Model { nodes = nodes ++ [(port, ph)], .. }
  (SpawnNode port, SpawnFailed _)  -> Model {..}
  (KillNode (port, _ph), Ack)      -> Model { nodes = filter ((/= port) . fst) nodes, .. }
  (Set i, Ack)                     -> Model { value = Just i, .. }
  (Read, Value _i)                 -> Model {..}
  (Incr, Ack)                      -> Model { value = succ <$> value, ..}
  (BreakConnection ph, Ack)        -> Model { isolated = Just ph, .. }
  (FixConnection _ph, Ack)         -> Model { isolated = Nothing, .. }

precondition :: Model Symbolic -> Action Symbolic -> Logic
precondition Model {..} act = case act of
  SpawnNode {}       -> length nodes .< 3
  KillNode  {}       -> length nodes .== 3
  Set i              -> length nodes .== 3 .&& i .>= 0
  Read               -> length nodes .== 3 .&& Boolean (isJust value)
  Incr               -> length nodes .== 3 .&& Boolean (isJust value)
  BreakConnection {} -> length nodes .== 3 .&& Boolean (isNothing isolated)
  FixConnection   {} -> length nodes .== 3 .&& Boolean (isJust isolated)

postcondition :: Model Concrete -> Action Concrete -> Response Concrete -> Logic
postcondition Model {..} act resp = case (act, resp) of
  (Read, Value (Right i))        -> Just i .== value
  (Read, Value (Left e))         -> Bot .// e
  (SpawnNode {}, SpawnedNode {}) -> Top
  (SpawnNode {}, SpawnFailed {}) -> Bot .// "SpawnFailed"
  (KillNode {}, Ack)             -> Top
  (Set {}, Ack)                  -> Top
  (Incr {}, Ack)                 -> Top
  (BreakConnection {}, Ack)      -> Top
  (FixConnection {}, Ack)        -> Top
  (_,            _)              -> Bot .// "postcondition"

command :: ((Handle, Handle), Handle) -> String -> IO String
command ((sin, sout), log) cmd = do
  hPutStrLn log cmd
  hPutStrLn sin cmd
  resp <- getResponse sout
  if "system doesn't have a leader" `isInfixOf` resp
  then do
    hPutStrLn log "No leader, retrying..."
    threadDelay 500000
    command ((sin, sout), log) cmd
  else do
    hPutStrLn log ("got response `" ++ resp ++ "'")
    return resp

getResponse :: Handle -> IO String
getResponse h = do
  line <- fromMaybe (error "timedGetLine") <$> timeout 10000000 (hGetLine h)
  if "New leader found" `isInfixOf` line
  then getResponse h
  else return line

semantics :: ((Handle, Handle), Handle) -> Action Concrete -> IO (Response Concrete)
semantics ((_sin, _sout), log) (SpawnNode port1) = do
  hPutStrLn log "Spawning node"
  removePathForcibly ("/tmp/raft-log-" ++ show port1 ++ ".txt")
  log' <- openFile ("/tmp/raft-log-" ++ show port1 ++ ".txt") WriteMode
  let (port2, port3) = case port1 of
        3000 -> (3001, 3002)
        3001 -> (3000, 3002)
        3002 -> (3000, 3001)
  (_, _, _, ph) <- createProcess_ "raft node"
    (proc "fiu-run" [ "-x", "stack", "exec", "raft-example"
                    , "localhost:" ++ show port1
                    , "localhost:" ++ show port2
                    , "localhost:" ++ show port3
                    ])
      { std_out = UseHandle log'
      , std_err = UseHandle log'
      }
  threadDelay 1000000
  mec <- getProcessExitCode ph
  case mec of
    Nothing -> return (SpawnedNode (reference (Opaque ph)))
    Just ec -> do
      hPutStrLn log (show ec)
      return (SpawnFailed port1)
semantics ((_sin, _sout), log) (KillNode (_port, ph)) = do
  hPutStrLn log "Killing node"
  terminateProcess (opaque ph)
  return Ack
semantics hs (Set i) = do
  _resp <- command hs ("set x " ++ show i)
  return Ack
semantics hs Read = do
  -- threadDelay 1000000
  resp <- command hs "read"
  let parse = readEither
            . takeWhile isDigit
            . drop 1
            . snd
            . break (== ',')
  return (Value (bimap (++ (": " ++ resp)) id (parse resp)))
semantics hs Incr = do
  _resp <- command hs "incr x"
  return Ack
semantics ((_sin, _sout), log) (BreakConnection (port, ph)) = do
  hPutStrLn log ("Break connection, port: " ++ show port)
  Just pid <- getPid (opaque ph)
  callCommand ("fiu-ctrl -c \"enable name=posix/io/net/send\" " ++ show pid)
  -- callCommand ("fiu-ctrl -c \"enable name=posix/io/net/recv\" " ++ show pid)
  threadDelay 2000000
  return Ack
semantics ((_sin, _sout), log) (FixConnection (port, ph)) = do
  hPutStrLn log ("Fix connection, port: " ++ show port)
  Just pid <- getPid (opaque ph)
  callCommand ("fiu-ctrl -c \"disable name=posix/io/net/send\" " ++ show pid)
  -- callCommand ("fiu-ctrl -c \"disable name=posix/io/net/recv\" " ++ show pid)
  threadDelay 2000000
  return Ack

generator :: Model Symbolic -> Gen (Action Symbolic)
generator Model {..}
  | length nodes < 3 = SpawnNode <$> elements ([3000..3002] \\ map fst nodes)
  | otherwise        = case value of
      Nothing -> Set <$> arbitrary
      Just _  -> frequency
                   [ (1, Set <$> arbitrary)
                   , (5, pure Incr)
                   , (3, pure Read)
                   -- , (1, KillNode <$> elements nodes)
                   , (1, BreakConnection <$> elements nodes)
                   , (1, FixConnection   <$> elements nodes)
                   ]

shrinker :: Action Symbolic -> [Action Symbolic]
shrinker (Set i) = [ Set i' | i' <- shrink i ]
shrinker _       = []

mock :: Model Symbolic -> Action Symbolic -> GenSym (Response Symbolic)
mock _m SpawnNode {}       = SpawnedNode <$> genSym
mock _m KillNode {}        = pure Ack
mock _m Set {}             = pure Ack
mock _m Read {}            = pure (Value (Right 0))
mock _m Incr {}            = pure Ack
mock _m BreakConnection {} = pure Ack
mock _m FixConnection {}   = pure Ack

setup :: IO ((Handle, Handle), Handle)
setup = do
  removePathForcibly "/tmp/raft-log.txt"
  log <- openFile "/tmp/raft-log.txt" WriteMode
  (Just sin, Just sout, _serr, ph) <- createProcess_ "raft client"
    (proc "stack" ["exec", "raft-example", "client"])
      { std_in  = CreatePipe
      , std_out = CreatePipe
      , std_err = UseHandle log
      }
  threadDelay 100000
  mapM_ (flip hSetBuffering NoBuffering) [sin, sout, log]
  hPutStrLn sin "addNode localhost:3000"
  hPutStrLn sin "addNode localhost:3001"
  hPutStrLn sin "addNode localhost:3002"
  return ((sin, sout), log)

clean :: ((Handle, Handle), Handle) -> IO ()
clean ((sin, sout), log) = do
  hClose sin
  hClose sout
  hClose log

sm :: ((Handle, Handle), Handle) -> StateMachine Model Action IO Response
sm handles = StateMachine initModel transition precondition postcondition
               Nothing generator Nothing shrinker (semantics handles) mock

sequential :: ((Handle, Handle), Handle) -> Property
sequential handles = noShrinking $
  forAllCommands sm' (Just 10) $ \cmds -> monadicIO $ do
    (hist, model, res) <- runCommands sm' cmds
    prettyCommands sm' hist (res === Ok)
    liftIO (mapM_ (terminateProcess . opaque . snd) (nodes model))
  where
    sm' = sm handles

unit_sequential :: IO ()
unit_sequential = bracket setup clean (verboseCheck . sequential)

type Handles = ((Handle, Handle), Handle)

runOnce :: Commands Action -> Handles -> Property
runOnce cmds handles = once $ monadicIO $ do
  (hist, model, res) <- runCommands (sm handles) cmds
  prettyCommands (sm handles) hist (res === Ok)
  liftIO (mapM_ (terminateProcess . opaque . snd) (nodes model))

test0 :: Handles -> Property
test0 = runOnce cmds
  where
    cmds = Commands
      [ Command (SpawnNode 3000)
                (Set.fromList [Var 0])
      , Command (SpawnNode 3001)
                (Set.fromList [Var 1])
      , Command (SpawnNode 3002)
                (Set.fromList [Var 2])
      , Command (Set 1)
                Set.empty
      , Command Read
                Set.empty
      , Command Incr
                Set.empty
      , Command Read
                Set.empty
      ]

unit_test0 :: IO ()
unit_test0 = bracket setup clean (verboseCheck . test0)

test1 :: Handles -> Property
test1 = runOnce cmds
  where
    cmds = Commands
      [ Command (SpawnNode 3002) (Set.fromList [ Var 0 ])
      , Command (SpawnNode 3000) (Set.fromList [ Var 1 ])
      , Command (SpawnNode 3001) (Set.fromList [ Var 2 ])
      , Command (Set 8) Set.empty
      , Command Incr Set.empty
      , Command
          (KillNode ( 3000 , Reference (Symbolic (Var 1)) )) Set.empty
      , Command (SpawnNode 3000) (Set.fromList [ Var 3 ])
      , Command Read Set.empty
      ]

unit_test1 :: IO ()
unit_test1 = bracket setup clean (verboseCheck . test1)

test2 :: Handles -> Property
test2 = runOnce cmds
  where
    cmds = Commands
      [ Command (SpawnNode 3002) (Set.fromList [ Var 0 ])
      , Command (SpawnNode 3001) (Set.fromList [ Var 1 ])
      , Command (SpawnNode 3000) (Set.fromList [ Var 2 ])
      , Command (Set 7) Set.empty
      , Command Incr Set.empty
      , Command
          (BreakConnection ( 3002 , Reference (Symbolic (Var 0)) ))
          Set.empty
      , Command
          (FixConnection ( 3002 , Reference (Symbolic (Var 0)) )) Set.empty
      , Command Read Set.empty
      ]

unit_test2 :: IO ()
unit_test2 = bracket setup clean (verboseCheck . test2)
