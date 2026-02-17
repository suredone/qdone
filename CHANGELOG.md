# Changelog

v2.2.0 (February 2026)
-----------------------

### New Features

- **Expose SQS message attributes to job subprocesses** ([#86](https://github.com/suredone/qdone/pull/86)) — Worker now passes SQS message attributes (e.g. `ApproximateReceiveCount`, `SentTimestamp`, custom attributes) as environment variables to child processes.
- **Add `ApproximateAgeOfOldestMessage` to monitor command** ([#85](https://github.com/suredone/qdone/pull/85)) — The `monitor` command now includes queue age metrics alongside message counts.

### Bug Fixes

- **Fix FIFO serial-execution using wrong SQS attribute name** ([#88](https://github.com/suredone/qdone/pull/88)) — FIFO serial execution was setting the wrong attribute, preventing single-concurrency group processing from working correctly.
- **Fix `validateMessageOptions` rejecting per-message `delay` option** ([#90](https://github.com/suredone/qdone/pull/90)) — The `delay` option was incorrectly rejected when passed as a per-message option in `enqueue-batch`.
- **Fix dedup tests failing without local Redis** ([#91](https://github.com/suredone/qdone/pull/91)) — `test/dedup.test.js` now uses ioredis-mock consistently, and patches ZADD GT/LT flag support missing from ioredis-mock 8.x.

### Maintenance

- Move `ioredis-mock` and `standard` to devDependencies ([#89](https://github.com/suredone/qdone/pull/89))
- Fix lint warnings (unused imports, padded block)

v2.1.1 (February 2026)
-----------------------

### Bug Fixes

- **Fix missing CommonJS build in 2.1.0 package** — The 2.1.0 release was published without the `commonjs/` build artifacts, causing `require('qdone/commonjs')` to fail with `Cannot find module`. This patch re-includes the full CJS build.

v2.1.0 (February 2026)
-----------------------

First stable v2 release. v2 has been running in production since late 2022 across 56 alpha releases.

### Bug Fixes

- **Fix QueueDoesNotExist cache invalidation for `sendMessage`** ([#70](https://github.com/suredone/qdone/pull/70)) — When SQS returns `QueueDoesNotExist` (e.g. after `idle-queues --delete` removes a queue), qdone now invalidates the stale in-memory QRL cache, recreates the queue, and retries. Previously the stale URL was retried until backoff exhaustion.
- **Fix QueueDoesNotExist cache invalidation for `sendMessageBatch`** ([#70](https://github.com/suredone/qdone/pull/70)) — Same fix applied to the batch enqueue path.
- **Fix `check` command destructuring bug** ([#71](https://github.com/suredone/qdone/pull/71)) — `checkFailQueue` and `checkQueue` were incorrectly destructuring params.
- **Add `check` to CLI help text** ([#71](https://github.com/suredone/qdone/pull/71)) — The `check` subcommand was functional but missing from `qdone --help`.

v2.0.x-alpha (2022–2026)
-------------------------

Major rewrite of qdone. All releases published under the `next` npm tag.

### Breaking Changes

- Migrated from AWS SDK v2 to AWS SDK v3 (`@aws-sdk/client-sqs`, `@aws-sdk/client-cloudwatch`)
- Converted to ES modules (`"type": "module"` in package.json). CommonJS consumers must import from `qdone/commonjs`.
- Node.js >= 16 required

### New Features

- **Scheduler system** — New `SystemMonitor`, `QueueManager`, and `JobExecutor` architecture for managing workers at scale
- **`monitor` command** ([#67](https://github.com/suredone/qdone/pull/67)) — Monitor multiple queues at once with prefix/suffix validation and FIFO support
- **`check` command** ([#62](https://github.com/suredone/qdone/pull/62)) — Verify queue configuration matches expected attributes
- **DLQ support** — Dead letter queues with `--dlq`, `--dlq-suffix`, and `--dlq-after` options
- **External deduplication** ([#61](https://github.com/suredone/qdone/pull/61), [#64](https://github.com/suredone/qdone/pull/64)) — Redis-backed dedup with return of deduplication IDs to callers
- **`--delay` option** — Delay delivery of messages by up to 900 seconds
- **`--tag` option** — Add AWS tags to queues at creation time
- **`--archive` flag** — Drain jobs to stdout
- **`--fail-delay` option** — Delay on the entire fail queue
- **Environment variable configuration** — All options configurable via env vars
- **Sentry integration** — `--sentry-dsn` option for error tracking
- **Memory control** — Configurable memory limits for workers

### Improvements

- Idle queue deletion improvements ([#66](https://github.com/suredone/qdone/pull/66)) — Including DLQ cleanup and orphan deletion
- QRL cache and load calculation fixes ([#68](https://github.com/suredone/qdone/pull/68))
- Faster worker ramp-up, allow multiple jobs from the same queue
- Improved batch message handling and shared buffer fixes

v.1.7.0
-------

### New Features

#### Added `--deduplication-id` option for enqueue ([#40](https://github.com/suredone/qdone/issues/40))

`qdone` has always set a deduplication id (using a UUID v1) when sending enqueue calls, but it looks like the aws sdk does not have adequate retry defaults set. This option lets a qdone user retry enqueue operations. For more information please see the [AWS docs for Message Deduplication ID](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagededuplicationid-property.html).

### Under the hood

- Updated aws-sdk.
- Updated locked dependencies.

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
