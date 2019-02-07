
module Control.Concurrent.STM.Timer (
  Timer,
  newTimer,
  newTimerRange,
  startTimer,
  resetTimer,
  waitTimer,
) where

import Protolude

import Control.Concurrent.Async
import Control.Concurrent.STM
import System.Random (StdGen, randomR, mkStdGen)

import Numeric.Natural

data Timer = Timer
  { timerAsync :: TMVar (Async ())
    -- ^ The async computation of the timer
  , timerLock :: TMVar ()
    -- ^ When the TMVar is empty, the timer is being used
  , timerGen :: TVar StdGen
  , timerRange :: (Natural, Natural)
  }

-- | Create a new timer with the supplied timer action and timer length,
newTimer :: Natural -> IO Timer
newTimer timeout = newTimerRange 0 (timeout, timeout)

-- | Create a new timer with the supplied timer action, random seed, and range
-- from which the the timer will choose a random timer length at each
-- start or reset.
newTimerRange :: Int -> (Natural, Natural) -> IO Timer
newTimerRange seed timeoutRange = do
  (timerAsync, timerLock, timerGen) <-
    atomically $ (,,) <$> newEmptyTMVar <*> newTMVar () <*> newTVar (mkStdGen seed)
  pure $ Timer timerAsync timerLock timerGen timeoutRange

--------------------------------------------------------------------------------

-- | Start the timer. If the timer is already running, the timer is not started.
-- Returns True if the timer was succesfully started.
startTimer :: Timer -> IO Bool
startTimer timer = do
  mlock <- liftIO . atomically $ tryTakeTMVar (timerLock timer)
  case mlock of
    Nothing -> pure False
    Just () -> resetTimer timer >> pure True

-- | Resets the timer with a new random timeout.
resetTimer :: Timer -> IO ()
resetTimer timer = do

  -- Check if a timer is already running. If it is, asynchronously kill the
  -- thread.
  mta <- atomically $ tryTakeTMVar (timerAsync timer)
  case mta of
    Nothing -> pure ()
    Just ta -> void $ async (uninterruptibleCancel ta)

  -- Fork a new async computation that waits the specified (random) amount of
  -- time, performs the timer action, and then puts the lock back signaling the
  -- timer finishing.
  ta <- async $ do
    threadDelay =<< randomDelay timer
    success <- atomically $ tryPutTMVar (timerLock timer) ()
    when (not success) $
      panic "[Failed Invariant]: Putting the timer lock back should succeed"

  -- Check that putting the new async succeeded. If it did not, there is a race
  -- condition and the newly created async should be canceled. Warning: This may
  -- not work for _very_ short timers.
  success <- atomically $ tryPutTMVar (timerAsync timer) ta
  when (not success) $
    void $ async (uninterruptibleCancel ta)

-- | Wait for a timer to complete
waitTimer :: Timer -> IO ()
waitTimer timer = atomically $ readTMVar (timerLock timer)

--------------------------------------------------------------------------------

randomDelay :: Timer -> IO Int
randomDelay timer = atomically $ do
  g <- readTVar (timerGen timer)
  let (tmin, tmax) = timerRange timer
      (n, g') = randomR (toInteger tmin, toInteger tmax) g
  writeTVar (timerGen timer) g'
  pure (fromIntegral n)
