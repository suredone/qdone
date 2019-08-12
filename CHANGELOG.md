# Changelog

v.1.6.0
-------

### New Features

#### Caching for SQS `GetQueueAttributes` calls ([#41](https://github.com/suredone/qdone/issues/41))

After switching our infrastructure to `--active-only` on jobs that have a large number of dynamic queues, we noticed that spend a lot of money on GetQueueAttributes calls. However the state of the active queues is very cacheable, especially if queues tend to have large backlogs, as ours do.

We added the following options to the `idle-queues`, and `worker` commands to be used in conjunction with `--active-only`:

- `--cache-url` that takes a `redis://...` or a `redis-cluster://` url [no default]
- `--cache-ttl-seconds` that takes a number of seconds [default `10`]
- `--cache-prefix` that defines a cache key prefix [default `qdone:`]

The presence of the `--cache-url` option will cause the worker to cache `GetQueueAttributes` for each queue for the specified ttl.


v.1.5.0
-------

### New Features

#### Added `--group-id-per-message` option for `enqueue-batch` ([#33](https://github.com/suredone/qdone/issues/33))

This option creates a new Group ID for every message in a batch, for when you want exactly once delivery, but don't care about message order.

### Bug Fixes

- Fixed ([#35](https://github.com/suredone/qdone/issues/35)) by making `idle-queues` pairing behavior work for FIFO queues as well as normal queues.


v.1.4.0
-------

### Bug Fixes

- Fixed ([#25](https://github.com/suredone/qdone/issues/25)) bug on Linux in `worker` where child processes were not getting killed after `--kill-after` timer was reached.


v.1.3.0
-------

### New Features

#### FIFO Option ([#18](https://github.com/suredone/qdone/issues/18))

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

#### Only Listen To Active Queues with `--active-only`

We encountered an occasional production problem where aggressively deleting idle queues can cause the loss of a message that was sent between the idle check and the delete operation. We were using `qdone idle-queues --delete --idle-for 10`, which is much more aggressive than the default of 60 minutes.

To address this, we are adding an alternate mode of operation to the worker with the new `--active-only` flag for use with wildcard (`*`) queues that does a cheap SQS API call to check whether a queue currently has waiting messages. If so, it's put into the list of queues for the current listening round. This should have the net effect of reducing the number of queues workers have to listen to (similarly to aggresive usage of `qdone idle-queues --delete`) without exposing messages to the delete race condition. For cases where idle queues still must be deleted, we recommend using a longer timeout.

### Bug Fixes

- Fixed ([#29](https://github.com/suredone/qdone/issues/29)) bug in `enqueue-batch` where SQS batches where command lines added up to > 256kb would not be split correctly and loop

### Under the hood

- Increased test coverage related to ([#29](https://github.com/suredone/qdone/issues/29))
- Added test coverage for ([#18](https://github.com/suredone/qdone/issues/18))
- Updated command line args libraries


v1.2.0 (January 5, 2018)
---------------------------

### Bug Fixes

- [#22](https://github.com/suredone/qdone/issues/22) fixes exception deleting failed queues in paired mode when fail queue does not exist


v1.1.0 (December 25, 2017)
-----------------------------

### New Features

- Add experimental support for using exports in node. Exports various functions from enqueue and worker for use from node. Doesn't change the public facing interface (which is command line only).


v1.0.0 (August 8, 2017)
--------------------------

### New Features

- There is a new command called [`idle-queues`](https://github.com/suredone/qdone#idle-queues-usage) which can identify queues that have had no activity for a specified period of time, and delete them, if desired.
- Qdone's `worker` now [allows a child process to finish running](https://github.com/suredone/qdone#shutdown-behavior) before shutting down in response to a `SIGTERM` or `SIGINT`.
- Queues are now always resolved, and the `--always-resolve` option has been removed.
- Output to non TTYs is less chatty by default, but you can get the previous behavior by using `--verbose`, or silence output in a TTY by using `--quiet`.
