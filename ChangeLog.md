# Changelog for raft

## 0.4.2.0 (Pending)

- Feature: Client write requests are now validated by the leader before being
  written to its log using the functions from the `RaftStateMachine` and
  `RaftStateMachinePure` typeclasses; Followers do not re-validate the log
  entry when they receive the entry from the leader.
- API change: The `clientSendX` family of functions now take a `ClientReq`
  value as an argument instead of a fully formed `ClientRequest` value, such
  that the known `ClientId` is used to construct the `ClientRequest` value
  instead of users having to supply it themselves.
- API change: The `RaftStateMachinePureError sm v` type family from the
  `RaftStateMachinePure` typeclass must now have both a `Show` and a
  `Serialize` instance.
- API change: Client write requests now have the potential of returning a
  `ClientWriteRespFail` signifying a failure during the validation of the
  request state machine update.
- Bug Fix: When running networks of size 2, 'incrCommitIndex' will no longer run
  in an infinite loop when incrementing the leader's commit to the correct N. 
- Bug Fix: When an invalid client request is submitted, the leader appropriately
  responds with a failure response and handles the next event as expected.

## 0.4.1.0

- Improvement: Users can now supply an existing logging function to log internal
  raft node logs, useful for integration into existing applications.
- Improvement: The example instances for `RaftClientSend` and
  `RaftClientRecv` using unix sockets no longer need to fork a server to handle
  responses from the raft node; the response is sent on the same socket the
  client opens while sending the request.

## 0.4.0.0

- API change: `MonadRaftAsync` is now `MonadRaftFork`, with a simpler API

## 0.3.0.0

- API change: `runRaftNode` now requires the monad it runs in to provide an
  instance of the `MonadRaftAsync` and `MonadRaftChan` in the `Raft.Monad`
  module in lieu of the previously necessary `MonadConc` instance.
- API change: Removed the `MonadConc` constraint on all example monad
  transformers and implemented inherited `MonadRaftChan` and `MonadRaftAsync` 
  instances 
- API change: Removed the `MonadConc` const
- API change: Renamed the old `Raft.Monad` module to `Raft.Transtion` and moved the 
  `RaftT` monad transformer from `Raft.hs` to the `Raft.Monad` module
- Improvement: Rework example monad `RaftExampleT` for simplicity


## 0.2.0.0

- Feature: Client requests are now cached by the current leader such that duplicate
  client requests are not serviced
- Bug Fix: Fix issue in concurrent timer not resetting properly when the timeout
  was set to values under 500ms
- Feature: Client read requests can now query entries by index or range of
  indices
- Improvement: Raft nodes now only write to disk if the state changes during
  handling of events
- Feature: Users can now specify what severity of log messages should be logged
- Bug Fix: Receiving all data from a socket in the RaftSocketT monad
  transformer RaftRecvX typeclass instances inducing a deadlock has been
  rewritten
- Feature: A PostgreSQL backend has been added for storage of log entries

## 0.1.2.0

- Added a heartbeat broadcast by leader on read requests from client to ensure
  that the node is leader before responding to client
- Each log entry is now linked by sha256 hash to the log entry immediately
  preceding it
- Fixed bug where the last log entry was not being read from disk before
  starting the main event loop
- Added quickcheck-state-machine tests for black-box testing of the raft-example
  client program

## 0.1.1.0

- Fixed MonadFail constraints for GHC 8.6

## 0.1.0.0

- Initial release.
