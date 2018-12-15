# Changelog for raft

## 0.1.2

- Added a heartbeat broadcast by leader on read requests from client to ensure
  that the node is leader before responding to client
- Each log entry is now linked by sha256 hash to the log entry immediately
  preceding it
- Fixed bug where the last log entry was not being read from disk before
  starting the main event loop
- Added quickcheck-state-machine tests for black-box testing of the raft-example
  client program

## 0.1.1

- Fixed MonadFail constraints for GHC 8.6

## 0.1

- Initial release.
