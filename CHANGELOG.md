# Changelog

v.1.3.0-alpha (Unreleased)
--------------------------

## New Features

### FIFO Option ([#18])

Added a `--fifo` and `--group-id <string>` option to `equeue` and `enqueue-batch`
- Causes any new queues to be created as FIFO queues
- Causes the `.fifo` suffix to be appended to any queue names that do not explicitly have them
- Causes failed queues to take the form `${name}_failed.fifo`
- Any commands with the same `--group-id` will be worked on in the order they were received by SQS (see [FIFO docs](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html))
- If you don't set `--group-id` it defaults to a unique id per call to `qdone`, so this means messages sent by `enqueue-batch` will always be ordered as you sent them.
- There is NO option to set group id per-message in `enqueue-batch`. Adding this feature in the future will change the format of the batch input file.
- There is NO support right now for Content Deduplication, however a Unique Message Deduplication ID is generated for each command, so retry-able errors should not result in duplicate messages.

Added a `--fifo` option to `worker`
- Causes the `.fifo` suffix to be appended to any queue names that do not explicitly have them
- When wildcard names are specified (e.g. `test_*` or `*`), worker only listens to queues with a `.fifo` suffix.
- Failed queues are still only included if `--include-failed` is set.
- Regardless of how many workers you have, FIFO commands with the same `--group-id` will only be executed by one worker at a time.
- There is NO support right now for only-once processing using the Receive Request Attempt ID

## Bug Fixes

- Fixed ([#29]) bug in `enqueue-batch` where SQS batches where command lines added up to > 256kb would not be split correctly and loop

## Under the hood

- Increased test coverage related to ([#29])
- Added test coverage for ([#18])
- Updated command line args libraries

[#18](https://github.com/suredone/qdone/issues/18)
[#29](https://github.com/suredone/qdone/issues/29)


[v1.2.0]  (January 5, 2018)
---------------------------

## Bug Fixes

- [#22](https://github.com/suredone/qdone/issues/22) fixes exception deleting failed queues in paired mode when fail queue does not exist


[v1.1.0]. (December 25, 2017)
-----------------------------

## New Features

- Add experimental support for using exports in node. Exports various functions from enqueue and worker for use from node. Doesn't change the public facing interface (which is command line only).


[v1.0.0]. (August 8, 2017)
--------------------------

## New Features

- There is a new command called [`idle-queues`](https://github.com/suredone/qdone#idle-queues-usage) which can identify queues that have had no activity for a specified period of time, and delete them, if desired.
- Qdone's `worker` now [allows a child process to finish running](https://github.com/suredone/qdone#shutdown-behavior) before shutting down in response to a `SIGTERM` or `SIGINT`.
- Queues are now always resolved, and the `--always-resolve` option has been removed.
- Output to non TTYs is less chatty by default, but you can get the previous behavior by using `--verbose`, or silence output in a TTY by using `--quiet`.
