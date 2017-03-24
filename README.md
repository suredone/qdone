# qdone - Language agnostic job queue for SQS

[![](https://img.shields.io/david/suredone/qdone.svg)](https://david-dm.org/suredone/qdone)
[![Standard - JavaScript Style Guide](https://img.shields.io/badge/code_style-standard-brightgreen.svg)](https://standardjs.com)


This system is inspired in part by experience running RQ in production.

Enqueuing a job creates an associated queue on SQS, along with a fail queue
for jobs that don't return success, or run longer than the time you specify 

## Installing

    npm install -g qdone

## Examples

### Enqueue a job and run it on a worker

To enqueue a job, you just need a queue name and the text of a command to run:

    you@some-machine $ qdone enqueue example_queue "echo hi"
    Creating fail queue queue_name_failed
    Looking up attributes for https://sqs.us-east-1.amazonaws.com/405670151149/qdone_queue_name_failed
    Creating queue queue_name
    Enqueued job 2732eacd-15fd-44c4-8cc6-192b6a4dc7b4

Notice that qdone created the queue and the fail queue for us.

Run the job by starting a worker with the queue name you used in the previous step.

    you@other-machine $ qdone worker example_queue
    Resolving queues:
      done

    Listening to queues (in this order):
      example_queue - https://sqs.us-east-1.amazonaws.com/405670151149/qdone_example_queue

    Looking for work on example_queue (https://sqs.us-east-1.amazonaws.com/405670151149/qdone_example_queue)
      Found job 0066f8ad-8a90-4220-8c08-d01c2dd1d6d5
      Executing job command: nice echo hi
      SUCCESS
      stdout: hi
      cleaning up (removing job) ...
      done

### Enqueue jobs in bulk

You can batch up multiple jobs in a file or stdin using a queue
name and command on each line:
  
    you@some-machine $ qdone enqueue-batch -
    queue_0 true
    queue_1 true
    queue_2 true
    queue_3 true
    queue_4 true
    queue_5 true
    queue_6 true
    queue_7 true
    queue_8 true
    queue_9 true
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
    queue_one true
    queue_one true
    queue_one true
    queue_one true
    queue_two true
    queue_two true
    queue_two true
    queue_two true
    Enqueued job fb2fa6d1-a51d-4fbc-8284-b5da1ca97d05 request 1
    Enqueued job 85bfbe92-cdbf-4fa1-908c-fe846fb508b6 request 1
    Enqueued job cea6d180-5d67-436e-b9b5-ca3f1d874afa request 1
    Enqueued job 9050fd34-2a76-4c39-94d7-cab060a219d1 request 1
    Enqueued job 4e729c18-6181-4bde-ab4a-1ee872fb52cc request 2
    Enqueued job 6dac2e4d-651f-4c7a-b09f-ad4ad1b0b1b1 request 2
    Enqueued job 0252ae4b-89c4-4426-8ad5-b1480bfdb3a2 request 2
    Enqueued job 95567365-12df-4322-b528-db0a641bf25c request 2
    Enqueued 8 jobs

### Failed jobs

If the command exists with a non-zero exit code, this is considered a failure, 
and the job will be placed on the failed queue. There is a failed queue for
each queue:

    $ qdone enqueue my_queue "false"
    Enqueued job 0e5957de-1e13-4633-a2ed-d3b424aa53fb;

    $ qdone worker my_queue
    Resolving queues:
      done

    Listening to queues (in this order):
      my_queue - https://sqs.us-east-1.amazonaws.com/405670151149/qdone_my_queue

    Looking for work on my_queue (https://sqs.us-east-1.amazonaws.com/405670151149/qdone_my_queue)
      Found job 0e5957de-1e13-4633-a2ed-d3b424aa53fb
      Executing job command: nice false
      FAILED
      code  : 1
      error : Error: Command failed: nice false

To retry failed jobs, listen on the corresponding fail queue:

    $ qdone worker my_queue_failed --include-failed
    Resolving queues:
      done

    Listening to queues (in this order):
      my_queue_failed - https://sqs.us-east-1.amazonaws.com/405670151149/qdone_my_queue_failed

    Looking for work on my_queue_failed (https://sqs.us-east-1.amazonaws.com/405670151149/qdone_my_queue_failed)
      Found job 0e5957de-1e13-4633-a2ed-d3b424aa53fb
      Executing job command: nice false
      FAILED
      code  : 1
      error : Error: Command failed: nice false

### Listening to multiple queues

It's a common use case to listen to a set of queues matching a given prefix:

    $ qdone worker test*
    Resolving queues:
      done

    Listening to queues (in this order):
      test - https://sqs.us-east-1.amazonaws.com/405670151149/qdone_test
      test1 - https://sqs.us-east-1.amazonaws.com/405670151149/qdone_test1
      test2 - https://sqs.us-east-1.amazonaws.com/405670151149/qdone_test2
      test3 - https://sqs.us-east-1.amazonaws.com/405670151149/qdone_test3
      test4 - https://sqs.us-east-1.amazonaws.com/405670151149/qdone_test4
      test5 - https://sqs.us-east-1.amazonaws.com/405670151149/qdone_test5
      test6 - https://sqs.us-east-1.amazonaws.com/405670151149/qdone_test6
      test7 - https://sqs.us-east-1.amazonaws.com/405670151149/qdone_test7
      test8 - https://sqs.us-east-1.amazonaws.com/405670151149/qdone_test8
      test9 - https://sqs.us-east-1.amazonaws.com/405670151149/qdone_test9

    Looking for work on test (https://sqs.us-east-1.amazonaws.com/405670151149/qdone_test)
      Found job 2486f4b5-57ef-4290-987c-7b1140409cc6
    ...
    Looking for work on test1 (https://sqs.us-east-1.amazonaws.com/405670151149/qdone_test1)
      Found job 0252ae4b-89c4-4426-8ad5-b1480bfdb3a2
    ...

The worker will listen to each queue for the `--wait-time` period, then start
over from the beginning.

If you need the worker to listen to queues created after it starts running, use
the `--always-resolve` option at the cost of and extra API call every round
through the queues.

## Usage

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

  AWS SQS Authentication:

    You must provide one of:
    1) On AWS instances: an IAM role that allows the appropriate SQS calls
    2) A credentials file (~/.aws/credentials) containing a [default]
       section with appropriate keys
    3) Both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY as environment
       variables

  Examples:

    $ qdone enqueue process-image "/usr/bin/php /var/myapp/process-image.php http://imgur.com/some-cool-cat.jpg"
    $ qdone worker process-image


## Worker usage

    Usage: worker [options] <queue...>

  Listen for work on one or more queues

  Options:

    -h, --help                  output usage information
    -k, --kill-after <seconds>  Kill job after this many seconds [30]
    -w, --wait-time <seconds>   Listen this long on each queue before moving to next [10]
    --always-resolve            Always resolve queue names that end in '*'. This can result
                                in more SQS calls, but allows you to listen to queues that
                                do not exist yet.
    --include-failed            When using '*' do not ignore fail queues.

  Details:

    If a queue name ends with the * (wildcard) character, worker will listen on all
    queues that match the name up-to the wildcard. Place arguments like this inside
    quotes to keep the shell from globbing local files.

  Examples:

    $ qdone worker process-images     # listen on a single queue
    $ qdone worker one two three      # listen on multiple queues
    $ qdone worker "process-images-*" # listen to both process-images-png and
                                      # process-images-jpeg if those queues exist

