{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE FlexibleInstances  #-}
{-# LANGUAGE PolyKinds          #-}
{-# LANGUAGE RecordWildCards    #-}
{-# LANGUAGE StandaloneDeriving #-}

module QuickCheckStateMachine where

import           Control.Concurrent            (threadDelay)
import           Control.Monad.IO.Class        (liftIO)
import           Data.Bifunctor                (bimap)
import           Data.Char                     (isDigit)
import           Data.List                     (isInfixOf, (\\))
import           Data.Maybe                    (isJust, isNothing)
import           Data.TreeDiff                 (ToExpr)
import           GHC.Generics                  (Generic, Generic1)
import           Prelude
import           System.Directory              (removePathForcibly)
import           System.IO                     (BufferMode (NoBuffering),
                                                Handle, IOMode (WriteMode),
                                                hClose, hGetLine, hPutStrLn,
                                                hPutStrLn, hSetBuffering,
                                                openFile)
import           System.Process                (ProcessHandle, StdStream (CreatePipe, UseHandle),
                                                callCommand, createProcess_,
                                                getPid, getProcessExitCode,
                                                proc, std_err, std_in, std_out,
                                                terminateProcess,
                                                withCreateProcess)
import           System.Timeout                (timeout)
import           Test.QuickCheck               (Gen, Property, arbitrary,
                                                elements, frequency,
                                                noShrinking, shrink,
                                                withMaxSuccess, (===))
import           Test.QuickCheck.Monadic       (monadicIO)
import           Test.StateMachine             (Concrete, GenSym, Logic (..),
                                                Opaque (..), Reason (Ok),
                                                Reference, StateMachine (..),
                                                Symbolic, forAllCommands,
                                                genSym, opaque, prettyCommands,
                                                reference, runCommands, (.&&),
                                                (.//), (.==), (.>=), (.<))
import qualified Test.StateMachine.Types.Rank2 as Rank2
import           Text.Read                     (readEither)

------------------------------------------------------------------------

type Port = Int

data Persistence = Fresh | Existing
  deriving (Show)

data Action (r :: * -> *)
  = SpawnNode Port Persistence
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
  | Timeout
  | Value (Either String Integer)
  deriving (Show, Generic1, Rank2.Foldable)

data Model (r :: * -> *) = Model
  { nodes    :: [(Port, Reference (Opaque ProcessHandle) r)]
  , started  :: Bool
  , value    :: Maybe Integer
  , isolated :: Maybe (Port, Reference (Opaque ProcessHandle) r)
  }
  deriving (Show, Generic)

deriving instance ToExpr (Model Concrete)

initModel :: Model r
initModel = Model [] False Nothing Nothing

transition :: Model r -> Action r -> Response r -> Model r
transition Model {..} act resp = case (act, resp) of
  (SpawnNode port _, SpawnedNode ph) ->
    let newNodes = nodes ++ [(port, ph)]
     in if length newNodes == 3
           then Model { nodes = newNodes, started = True, .. }
           else Model { nodes = newNodes, .. }
  (SpawnNode {}, SpawnFailed _)    -> Model {..}
  (KillNode (port, _ph), Ack)      -> Model { nodes = filter ((/= port) . fst) nodes, .. }
  (Set i, Ack)                     -> Model { value = Just i, .. }
  (Read, Value _i)                 -> Model {..}
  (Incr, Ack)                      -> Model { value = succ <$> value, ..}
  (BreakConnection ph, Ack)        -> Model { isolated = Just ph, .. }
  (FixConnection _ph, Ack)         -> Model { isolated = Nothing, .. }
  (Read, Timeout)                  -> Model {..}
  (Set {}, Timeout)                -> Model {..}
  (Incr {}, Timeout)               -> Model {..}
  (_, _)                           -> error "transition"

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
  (Read,    Timeout)             -> Boolean (isJust isolated) .// "Read timeout"
  (Set {},  Timeout)             -> Boolean (isJust isolated) .// "Set timeout"
  (Incr {}, Timeout)             -> Boolean (isJust isolated) .// "Incr timeout"
  (_,            _)              -> Bot .// "postcondition"

command :: Handle -> String -> IO (Maybe String)
command h cmd = go 20
  where
    go :: Int -> IO (Maybe String)
    go 0 = return Nothing
    go n = do
      let cp =
            (proc "stack" ["exec", "raft-example", "client"])
              { std_in  = CreatePipe
              , std_out = CreatePipe
              , std_err = CreatePipe -- XXX: UseHandle h
              }
      withCreateProcess cp $ \(Just hin) (Just hout) _herr _ph -> do
        threadDelay 100000
        hSetBuffering hin  NoBuffering
        hSetBuffering hout NoBuffering
        hPutStrLn hin "addNode localhost:3000"
        hPutStrLn hin "addNode localhost:3001"
        hPutStrLn hin "addNode localhost:3002"
        hPutStrLn h cmd
        hPutStrLn hin cmd
        mresp <- getResponse hout
        case mresp of
          Nothing   -> do
            hPutStrLn h ("timeout, retrying to send command: " ++ cmd)
            threadDelay 100000
            go (n - 1)
          Just resp ->
            if "system doesn't have a leader" `isInfixOf` resp
            then do
              hPutStrLn h "No leader, retrying..."
              threadDelay 100000
              go n
            else do
              hPutStrLn h ("got response `" ++ resp ++ "'")
              return (Just resp)
      where
        getResponse :: Handle -> IO (Maybe String)
        getResponse hout = do
          mline <- timeout 500000 (hGetLine hout)
          case mline of
            Nothing   -> return Nothing
            Just line ->
              if "New leader found" `isInfixOf` line
              then getResponse hout
              else return (Just line)

semantics :: Handle -> Action Concrete -> IO (Response Concrete)
semantics h (SpawnNode port1 p) = do
  hPutStrLn h "Spawning node"
  removePathForcibly ("/tmp/raft-log-" ++ show port1 ++ ".txt")
  h' <- openFile ("/tmp/raft-log-" ++ show port1 ++ ".txt") WriteMode
  let port2, port3 :: Int
      (port2, port3) = case port1 of
        3000 -> (3001, 3002)
        3001 -> (3000, 3002)
        3002 -> (3000, 3001)
        _    -> error "semantics: invalid port1"
  let persistence Fresh    = "fresh"
      persistence Existing = "existing"
  (_, _, _, ph) <- createProcess_ "raft node"
    (proc "fiu-run" [ "-x", "stack", "exec", "raft-example", "node"
                    , persistence p
                    , "localhost:" ++ show port1
                    , "localhost:" ++ show port2
                    , "localhost:" ++ show port3
                    ])
      { std_out = UseHandle h'
      , std_err = UseHandle h'
      }
  mec <- getProcessExitCode ph
  case mec of
    Nothing -> return (SpawnedNode (reference (Opaque ph)))
    Just ec -> do
      hPutStrLn h (show ec)
      return (SpawnFailed port1)
semantics h (KillNode (_port, ph)) = do
  hPutStrLn h "Killing node"
  terminateProcess (opaque ph)
  return Ack
semantics hs (Set i) = do
  mresp <- command hs ("set x " ++ show i)
  case mresp of
    Nothing    -> return Timeout
    Just _resp -> return Ack
semantics hs Read = do
  mresp <- command hs "read"
  case mresp of
    Nothing   -> return Timeout
    Just resp -> do
      let parse = readEither
                . takeWhile isDigit
                . drop 1
                . snd
                . break (== ',')
      return (Value (bimap (++ (": " ++ resp)) id (parse resp)))
semantics hs Incr = do
  mresp <- command hs "incr x"
  case mresp of
    Nothing    -> return Timeout
    Just _resp -> return Ack
semantics h (BreakConnection (port, ph)) = do
  hPutStrLn h ("Break connection, port: " ++ show port)
  Just pid <- getPid (opaque ph)
  callCommand ("fiu-ctrl -c \"enable name=posix/io/net/send\" " ++ show pid)
  callCommand ("fiu-ctrl -c \"enable name=posix/io/net/recv\" " ++ show pid)
  return Ack
semantics h (FixConnection (port, ph)) = do
  hPutStrLn h ("Fix connection, port: " ++ show port)
  Just pid <- getPid (opaque ph)
  callCommand ("fiu-ctrl -c \"disable name=posix/io/net/send\" " ++ show pid)
  callCommand ("fiu-ctrl -c \"disable name=posix/io/net/recv\" " ++ show pid)
  return Ack

generator :: Model Symbolic -> Gen (Action Symbolic)
generator Model {..}
  | length nodes < 3  =
      if started
        then flip SpawnNode Existing <$> elements ([3000..3002] \\ map fst nodes)
        else flip SpawnNode Fresh <$> elements ([3000..3002] \\ map fst nodes)
  | otherwise        = case value of
      Nothing -> Set <$> arbitrary
      Just _  -> frequency $
                   [ (1, Set <$> arbitrary)
                   , (5, pure Incr)
                   , (3, pure Read)
                   , (1, KillNode <$> elements nodes)
                   , (1, BreakConnection <$> elements nodes)
                   ] ++ case isolated of
                          Just node -> [(1, pure (FixConnection node))]
                          Nothing   -> []

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

setup :: IO Handle
setup = do
  removePathForcibly "/tmp/raft-log.txt"
  h <- openFile "/tmp/raft-log.txt" WriteMode
  hSetBuffering h NoBuffering
  return h

sm :: Handle -> StateMachine Model Action IO Response
sm h = StateMachine initModel transition precondition postcondition
               Nothing generator Nothing shrinker (semantics h) mock

prop_sequential :: Property
prop_sequential = withMaxSuccess 5 $ noShrinking $
  forAllCommands (sm undefined) (Just 20) $ \cmds -> monadicIO $ do
    h <- liftIO setup
    let sm' = sm h
    (hist, model, res) <- runCommands sm' cmds
    prettyCommands sm' hist (res === Ok)
    liftIO (hClose h)
    liftIO (mapM_ (terminateProcess . opaque . snd) (nodes model))
