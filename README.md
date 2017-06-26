[![NPM Package](https://img.shields.io/npm/v/qdone.svg)](https://www.npmjs.com/package/qdone)
[![Build Status](https://travis-ci.org/suredone/qdone.svg?branch=master)](https://travis-ci.org/suredone/qdone)
[![Coverage Status](https://coveralls.io/repos/github/suredone/qdone/badge.svg)](https://coveralls.io/github/suredone/qdone)
[![Dependencies](https://img.shields.io/david/suredone/qdone.svg)](https://david-dm.org/suredone/qdone)
[![Standard - JavaScript Style Guide](https://img.shields.io/badge/code_style-standard-brightgreen.svg)](https://standardjs.com)

# qdone

Command line job queue for SQS

## Features

  - Enqueue and run any command line job with parameters
  - Creates SQS queues (and failed job queues) on demand
  - Minimizes SQS API calls
  - Workers can listen to multiple queues, including wildcards
  - Efficient batch enqueing of large numbers of jobs
  - Dynamic visibility timeout for long running jobs

qdone was inspired, in part, by experiences with [RQ](http://python-rq.org) in production.

## Installing

    npm install -g qdone

## Examples

Enqueue a job and run it:

```bash
$ qdone enqueue myQueue "echo hello world"
Enqueued job 030252de-8a3c-42c6-9278-c5a268660384

$ qdone worker myQueue
...
Looking for work on myQueue (https://sqs.us-east-1ld...)
  Found job a23c71b3-b148-47b1-bfbb-f5dbb344ef97
  Executing job command: nice echo hello world
  SUCCESS
  stdout: hello world
```

Queues are automatically created when you use them:

```bash
$ qdone enqueue myNewQueue "echo nice to meet you"
Creating fail queue myNewQueue_failed
Creating queue myNewQueue
Enqueued job d0077713-11e1-4de6-8f26-49ad51e008b9
```

Notice that qdone also created a failed queue. More on that later.


To queue many jobs at once, put a queue name and command on each line of
stdin or a file:
  
```bash
$ qdone enqueue-batch -  # use stdin
queue_0 echo hi
queue_1 echo hi
queue_2 echo hi
queue_3 echo hi
queue_4 echo hi
queue_5 echo hi
queue_6 echo hi
queue_7 echo hi
queue_8 echo hi
queue_9 echo hi
^D
Enqueued job 14fe4e30-bd4f-4415-b902-8df29cb73066 request 1
Enqueued job 60e31392-9810-4770-bfad-6a8f44114287 request 2
Enqueued job 0f26806c-2030-4d9a-94d5-b8d4b7a89115 request 3
Enqueued job 330c3d93-0364-431a-961b-5ace83066e55 request 4
Enqueued job ef64ab68-889d-4214-9ba5-af70d84565e7 request 5
Enqueued job 0fece491-6092-4ad2-b77a-27ccb0bd8e36 request 6
Enqueued job f053b027-3f4a-4e6e-8bb5-729dc8ecafa7 request 7
Enqueued job 5f11b69e-ede1-4ea2-8a60-c994adf2c5a0 request 8
Enqueued job 5079a10a-b13c-4b31-9722-8c1d3b146c28 request 9
Enqueued job 5dfe1008-9a1e-41df-b3bc-614ec5f34660 request 10
Enqueued 10 jobs
```

If you are using the same queue, requests to SQS will be batched:

```bash
$ qdone enqueue-batch -  # use stdin
queue_one echo hi
queue_one echo hi
queue_one echo hi
queue_one echo hi
queue_two echo hi
queue_two echo hi
queue_two echo hi
queue_two echo hi
^D
Enqueued job fb2fa6d1... request 1   # one
Enqueued job 85bfbe92... request 1   # request
Enqueued job cea6d180... request 1   # for queue_one
Enqueued job 9050fd34... request 1   #
Enqueued job 4e729c18... request 2      # another
Enqueued job 6dac2e4d... request 2      # request
Enqueued job 0252ae4b... request 2      # for queue_two
Enqueued job 95567365... request 2      #
Enqueued 8 jobs
```

### Failed jobs

A command fails if it finishes with a non-zero exit code:

```bash
$ qdone enqueue myQueue "false"
Enqueued job 0e5957de-1e13-4633-a2ed-d3b424aa53fb;

$ qdone worker myQueue
...
Looking for work on myQueue (https://sqs.us-east-1....)
  Found job 0e5957de-1e13-4633-a2ed-d3b424aa53fb
  Executing job command: nice false
  FAILED
  code  : 1
  error : Error: Command failed: nice false
```

The failed command will be placed on the failed queue.

To retry failed jobs, wait 30 seconds, then listen to the corresponding
failed queue:

```bash
$ qdone worker myQueue_failed --include-failed
...
Looking for work on myQueue_failed (https://sqs.us-east-1.../qdone_myQueue_failed)
  Found job 0e5957de-1e13-4633-a2ed-d3b424aa53fb
  Executing job command: nice false
  FAILED
  code  : 1
  error : Error: Command failed: nice false
```

It failed again. It will go back on the failed queue.

In production you will either want to set alarms on the failed queue to make
sure that it doesn't grow to large, or set all your failed queues to drain to
a failed job queue after some number of attempts, which you also check.

### Listening to multiple queues

It's nice sometimes to listen to a set of queues matching a prefix:

```bash
$ qdone worker 'test*'  # use single quotes to keep shell from globbing
...
Listening to queues (in this order):
  test - https://sqs.us-east-1.../qdone_test
  test1 - https://sqs.us-east-1.../qdone_test1
  test2 - https://sqs.us-east-1.../qdone_test2
  test3 - https://sqs.us-east-1.../qdone_test3
  test4 - https://sqs.us-east-1.../qdone_test4
  test5 - https://sqs.us-east-1.../qdone_test5
  test6 - https://sqs.us-east-1.../qdone_test6
  test7 - https://sqs.us-east-1.../qdone_test7
  test8 - https://sqs.us-east-1.../qdone_test8
  test9 - https://sqs.us-east-1.../qdone_test9

Looking for work on test (https://sqs.us-east-1.../qdone_test)
  Found job 2486f4b5-57ef-4290-987c-7b1140409cc6
...
Looking for work on test1 (https://sqs.us-east-1.../qdone_test1)
  Found job 0252ae4b-89c4-4426-8ad5-b1480bfdb3a2
...
```

The worker will listen to each queue for the `--wait-time` period, then start
over from the beginning.

### Long running jobs

Workers prevent others from processing their job by automatically extending the
default SQS visibility timeout (30 seconds) as long as the job is still
running. You can see this when running a long job:

```bash
$ qdone enqueue test "sleep 35"
Enqueued job d8e8927f-5e42-48ae-a1a8-b91e42700942

$ qdone worker test --kill-after 300
...
  Found job d8e8927f-5e42-48ae-a1a8-b91e42700942
  Executing job command: nice sleep 35
  Ran for 15.009 seconds, requesting another 60 seconds
  SUCCESS
...
```

The SQS API call to extend this timeout (`ChangeMessageVisibility`) is called
at the halfway point before the message becomes visible again. The tiemout
doubles in length every subsequent call, but never exceeding `--kill-after`.

## Production Logging

The output examples in this readme assume you are running qdone from an interactive shell. However, if the shell is non-interactive (technically if stdout is not a tty) then qdone will automatically use the `--quiet` option and will log failures to stdout as one JSON object per line the following format:

```javascript
{
  "event": "JOB_FAILED",
  "timestamp": "2017-06-25T20:21:19.744Z",
  "job": "0252ae4b-89c4-4426-8ad5-b1480bfdb3a2",
  "command": "python /opt/myapp/jobs/reticulate_splines.py 42",
  "exitCode": "1",
  "killSignal": "SIGTERM",
  "stderr": "...",
  "stdout": "reticulating splines...",
  "errorMessage": "You can't kill me using SIGTERM, muwahahahahaha! Oh wait..."
}
```

Each field in the above JSON except `event` and `timestamp` is optional, and only appears when it contains data. Note that log events other than `JOB_FAILED` may be added in the future. Also note that warnings and errors not in the above JSON format will appear on stderr.


## SQS API Call Complexity

| Context | Calls | Details |
| -- | -- | -- |
| `qdone enqueue` |  2&nbsp;[+3] | One call to resolve the queue name, one call to enqueue the command, three extra calls if the queue does not exist yet. |
| `qdone enqueue-batch` |  **q**&nbsp;+&nbsp;ceil(**c**/10)&nbsp;+&nbsp;3**n** | **q**: number of unique queue names in the batch <br/> **c**: number of commands in the batch  <br/> **n**: number of queues that do not exist yet |
| `qdone worker` (while listening) |  <nobr>**n** + (1&nbsp;per&nbsp;**n**&times;**w**)</nobr> | **w**: `--wait-time` in seconds <br /> **n**: number of queues  |
| `qdone worker` (while job running) |  <nobr>log(**t**/30)&nbsp;+&nbsp;1</nobr> | **t**: total job run time in seconds |

## AWS Authentication

You must provide **ONE** of:

- On AWS instances, the instance may have an IAM role that allows the appropriate SQS calls. No further configuration necessary.
- A credentials file (~/.aws/credentials) containing a [default] section with appropriate keys.
- Both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY as environment variables

Example IAM policy allowing qdone to use queues with its prefix in any region:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "sqs:GetQueueAttributes",
                "sqs:GetQueueUrl",
                "sqs:SendMessage",
                "sqs:SendMessageBatch",
                "sqs:ReceiveMessage",
                "sqs:DeleteMessage",
                "sqs:CreateQueue",
                "sqs:ChangeMessageVisibility"
            ],
            "Effect": "Allow",
            "Resource": "arn:aws:sqs:*:YOUR_ACCOUNT_ID:qdone_*"
        },
        {
            "Action": [
                "sqs:ListQueues"
            ],
            "Effect": "Allow",
            "Resource": "arn:aws:sqs:*:YOUR_ACCOUNT_ID"
        }
    ]
}
```

## Command Line Usage

    usage: qdone [options] <command>

Commands

    enqueue         Enqueue a single command                       
    enqueue-batch   Enqueue multiple commands from stdin or a file 
    worker          Execute work on one or more queues             

Global Options

    --prefix string        Prefix to place at the front of each SQS queue name [default: qdone_]
    --fail-suffix string   Suffix to append to each queue to generate fail queue name [default: _failed]
    --region string        AWS region for Queues [default: us-east-1]
    -q, --quiet            Less verbose output suitible for production logging. Automatically
                           set if stdout is not a tty.
    -V, --version          Show version number
    --help                 Print full help message.

### Enqueue Usage

    usage: qdone enqueue [options] <queue> <command>
    usage: qdone enqueue-batch [options] <file...>

`<file...>` can be one ore more filenames or - for stdin 

Options

    --prefix string        Prefix to place at the front of each SQS queue name [default: qdone_]
    --fail-suffix string   Suffix to append to each queue to generate fail queue name [default: _failed]
    --region string        AWS region for Queues [default: us-east-1]
    -V, --version          Show version number
    --help                 Print full help message.

### Worker Usage

    usage: qdone worker [options] <queue...>

`<queue...>` one or more queue names to listen on for jobs 

If a queue name ends with the * (wildcard) character, worker will listen on all queues that match the name up-to the wildcard. Place arguments like this inside quotes to keep the shell from globbing local files.

  Options:

    -k, --kill-after number   Kill job after this many seconds [default: 30]
    -w, --wait-time number    Listen at most this long on each queue [default: 20]
    --include-failed          When using '*' do not ignore fail queues.
    --drain                   Run until no more work is found and quit. NOTE: if used with  --wait-time 0,
                              this option will not drain queues.
    --prefix string           Prefix to place at the front of each SQS queue name [default: qdone_]
    --fail-suffix string      Suffix to append to each queue to generate fail queue name [default: _failed]
    --region string           AWS region for Queues [default: us-east-1]
    -V, --version             Show version number
    --help                    Print full help message.

