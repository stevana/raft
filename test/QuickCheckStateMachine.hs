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
import           Data.Maybe                    (fromMaybe, isJust)
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
--  | BreakConnection Side (Reference (Opaque ProcessHandle) r)
--  | FixConnection Side (Reference (Opaque ProcessHandle) r)
  deriving (Show, Generic1, Rank2.Functor, Rank2.Foldable, Rank2.Traversable)

  {-
data Side = Sender | Receiver
  deriving Show
-}

data Response (r :: * -> *)
  = SpawnedNode (Reference (Opaque ProcessHandle) r)
  | SpawnFailed Port
  | Ack
  | Value (Either String Integer)
--  | BrokeConnection
--  | FixedConnection
  deriving (Show, Generic1, Rank2.Foldable)

data Model (r :: * -> *) = Model
  { nodes :: [(Port, Reference (Opaque ProcessHandle) r)]
  , value :: Maybe Integer
  }
  deriving (Show, Generic)

deriving instance ToExpr (Model Concrete)

initModel :: Model r
initModel = Model [] Nothing

transition :: Model r -> Action r -> Response r -> Model r
transition Model {..} (SpawnNode port) (SpawnedNode ph) =
  Model { nodes = nodes ++ [(port, ph)], .. }
transition Model {..} (SpawnNode port)    (SpawnFailed _)  = Model {..}
transition Model {..} (KillNode (port, _ph)) Ack              =
  Model { nodes = filter ((/= port) . fst) nodes, .. }
transition Model {..} (Set i)             Ack              =
  Model { value = Just i, .. }
transition Model {..} Read                (Value _i)       = Model {..}
transition Model {..} Incr                Ack              =
  Model { value = succ <$> value, ..}

precondition :: Model Symbolic -> Action Symbolic -> Logic
precondition Model {..} SpawnNode {} = length nodes .< 3
precondition Model {..} KillNode  {} = length nodes .== 3
precondition Model {..} (Set i)      = length nodes .== 3 .&& i .>= 0
precondition Model {..} Read         = length nodes .== 3 .&& Boolean (isJust value)
precondition Model {..} Incr         = length nodes .== 3 .&& Boolean (isJust value)

postcondition :: Model Concrete -> Action Concrete -> Response Concrete -> Logic
postcondition Model {..} Read         (Value (Right i)) = Just i .== value
postcondition Model {..} Read         (Value (Left e))  = Bot .// e
postcondition _model     SpawnNode {} SpawnedNode {}    = Top
postcondition _model     SpawnNode {} SpawnFailed {}    = Bot .// "SpawnFailed"
postcondition _model     KillNode {}  Ack               = Top
postcondition _model     Set {}       Ack               = Top
postcondition _model     Incr {}      Ack               = Top
postcondition _model     _            _                 = Bot .// "postcondition"

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
    -- (proc "fiu-run" [ "-x", "stack", "exec", "raft-example"
    (proc "stack"   [ "exec", "raft-example"
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
semantics ((_sin, _sout), log) (KillNode (_i, ph)) = do
  hPutStrLn log "Killing node"
  terminateProcess (opaque ph)
  return Ack
semantics hs (Set i) = do
  _resp <- command hs ("set x " ++ show i)
  return Ack
semantics hs Read = do
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
--semantics h (BreakConnection side ph) = do
--  threadDelay 200000
--  hPutStrLn h "Break connection"
--  Just pid <- getPid (opaque ph)
--  case side of
--    Sender    ->
--      callCommand ("fiu-ctrl -c \"enable name=posix/io/net/sendto\" " ++ show pid)
--    Receiver ->
--      callCommand ("fiu-ctrl -c \"enable name=posix/io/net/recvfrom\" " ++ show pid)
--  threadDelay 2000000
--  return BrokeConnection
--semantics h (FixConnection side ph) = do
--  threadDelay 200000
--  hPutStrLn h "Fix connection"
--  Just pid <- getPid (opaque ph)
--  case side of
--    Sender    ->
--      callCommand ("fiu-ctrl -c \"disable name=posix/io/net/sendto\" " ++ show pid)
--    Receiver ->
--      callCommand ("fiu-ctrl -c \"disable name=posix/io/net/recvfrom\" " ++ show pid)
--  threadDelay 1000000
--  return FixedConnection

generator :: Model Symbolic -> Gen (Action Symbolic)
generator Model {..}
  | length nodes < 3 = SpawnNode <$> elements ([3000..3002] \\ map fst nodes)
  | otherwise        = case value of
      Nothing -> Set <$> arbitrary
      Just _  -> frequency
                   [ (1, Set <$> arbitrary)
                   , (5, pure Incr)
                   , (3, pure Read)
                   , (1, KillNode <$> elements nodes)
                   ]

shrinker :: Action Symbolic -> [Action Symbolic]
shrinker (Set i) = [ Set i' | i' <- shrink i ]
shrinker _       = []

mock :: Model Symbolic -> Action Symbolic -> GenSym (Response Symbolic)
mock _m SpawnNode {} = SpawnedNode <$> genSym
mock _m KillNode {}  = pure Ack
mock _m Set {}       = pure Ack
mock _m Read {}      = pure (Value (Right 0))
mock _m Incr {}      = pure Ack
-- mock _m BreakConnection {}       = pure BrokeConnection
-- mock _m FixConnection {}         = pure FixedConnection

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

test0 :: ((Handle, Handle), Handle) -> Property
test0 handles = once $ monadicIO $ do
  (hist, model, res) <- runCommands (sm handles) cmds
  prettyCommands (sm handles) hist (res === Ok)
  liftIO (mapM_ (terminateProcess . opaque . snd) (nodes model))
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

test1 :: ((Handle, Handle), Handle) -> Property
test1 handles = once $ monadicIO $ do
  (hist, model, res) <- runCommands (sm handles) cmds
  prettyCommands (sm handles) hist (res === Ok)
  liftIO (mapM_ (terminateProcess . opaque . snd) (nodes model))
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
