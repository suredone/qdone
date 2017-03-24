# qdone

Language agnostic job queue for SQS

[![NPM Package](https://img.shields.io/npm/v/qdone.svg)](https://www.npmjs.com/package/qdone)
[![Dependencies](https://img.shields.io/david/suredone/qdone.svg)](https://david-dm.org/suredone/qdone)
[![Standard - JavaScript Style Guide](https://img.shields.io/badge/code_style-standard-brightgreen.svg)](https://standardjs.com)


## Features

  - Creates SQS queues (and failed job queues) on demand
  - Minimizes SQS API calls
  - Workers can listen to multiple queues, including wildcards
  - Efficient batch enqueing of large numbers of jobs

qdone was inspired, in part, by experiences with [RQ](http://python-rq.org) in production.

## Installing

    npm install -g qdone

## Examples

### Enqueue a job and run it

    $ qdone enqueue myQueue "echo hello world"
    Enqueued job 030252de-8a3c-42c6-9278-c5a268660384

    $ qdone worker myQueue
    ...
    Looking for work on myQueue (https://sqs.us-east-1ld...)
      Found job a23c71b3-b148-47b1-bfbb-f5dbb344ef97
      Executing job command: nice echo hello world
      SUCCESS
      stdout: hello world


### Queues are automatically created when you use them

    $ qdone enqueue myNewQueue "echo nice to meet you"
    Creating fail queue myNewQueue_failed
    Looking up attributes for https://sqs.us-east-1.../qdone_myNewQueue_failed
    Creating queue myNewQueue
    Enqueued job d0077713-11e1-4de6-8f26-49ad51e008b9

Notice that qdone also created a failed queue. More on that later.


### Enqueue many jobs at once

Put a queue name and command on each line of stdin or a file:
  
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

If you are using the same queue, requests to SQS will be batched:

    $ qdone enqueue-batch -
    queue_one echo hi
    queue_one echo hi
    queue_one echo hi
    queue_one echo hi
    queue_two echo hi
    queue_two echo hi
    queue_two echo hi
    queue_two echo hi
    Enqueued job fb2fa6d1... request 1   # one
    Enqueued job 85bfbe92... request 1   # request
    Enqueued job cea6d180... request 1   # for queue_one
    Enqueued job 9050fd34... request 1   #
    Enqueued job 4e729c18... request 2      # another
    Enqueued job 6dac2e4d... request 2      # request
    Enqueued job 0252ae4b... request 2      # for queue_two
    Enqueued job 95567365... request 2      #
    Enqueued 8 jobs


### Failed jobs

A command fails if it finishes with a non-zero exit code:

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

The failed command will be placed on the failed queue.


### Retrying failed jobs

Wait 30 seconds, then listen to the corresponding failed queue:

    $ qdone worker myQueue_failed --include-failed
    ...
    Looking for work on myQueue_failed (https://sqs.us-east-1.../qdone_myQueue_failed)
      Found job 0e5957de-1e13-4633-a2ed-d3b424aa53fb
      Executing job command: nice false
      FAILED
      code  : 1
      error : Error: Command failed: nice false

It failed again. It will go back on the failed queue.


### Listening to multiple queues

It's nice sometimes to listen to a set of queues matching a prefix:

    $ qdone worker test*
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

The worker will listen to each queue for the `--wait-time` period, then start
over from the beginning.

If you need to listen to queues *created after* the worker starts running, 
use the `--always-resolve` option. This costs an extra API call every round
through the queues.



## AWS Authentication

You must provide one of:

  1) On AWS instances, the instance may have an IAM role that allows
     the appropriate SQS calls. No further configuration necessary.
  2) A credentials file (~/.aws/credentials) containing a [default]
     section with appropriate keys.
  3) Both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY as environment
     variables


## Command Line Usage

    Usage: qdone [options] [command]

  Commands:

    enqueue <queue> <command>    Enqueue command on the specified queue
    enqueue-batch [file]         Read a list of (queue, command) pairs, one
                                 per line, from [file...] or - for stdin.
    worker [options] <queue...>  Listen for work on one or more queues

  Options:

    -h, --help              output usage information
    -V, --version           output the version number
    --prefix <prefix>       Prefex to place at the front of each SQS queue
                            name [qdone_]
    --fail-suffix <suffix>  Suffix to append to each queue to generate fail 
                            queue name [_failed]
    --region <region>       AWS region for Queues


## Worker Usage

    Usage: worker [options] <queue...>

  Listen for work on one or more queues

  Options:

    -h, --help                  output usage information
    -k, --kill-after <seconds>  Kill job after this many seconds [30]
    -w, --wait-time <seconds>   Listen this long on each queue before moving
                                to next [10]
    --always-resolve            Always resolve queue names that end in '*'.
                                This can result in more SQS calls, but allows
                                you to listen to queues that do not exist yet.
    --include-failed            When using '*' do not ignore fail queues.

  Details:

    If a queue name ends with the * (wildcard) character, worker will listen
    on all queues that match the name up-to the wildcard. Place arguments
    like this inside quotes to keep the shell from globbing local files.

  Examples:

    $ qdone worker process-images     # listen on a single queue
    $ qdone worker one two three      # listen on multiple queues
    $ qdone worker "process-images-*" # listen to both process-images-png and
                                      # process-images-jpeg if those queues exist
