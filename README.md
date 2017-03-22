# qdone

Simple, scalable queueing system for SQS.

    Usage: qdone [options] [command]

  Commands:

    enqueue <queue> <command>    Enqueue command on the specified queue
    worker [options] <queue...>  Listen for work on one or more queues

  Options:

    -h, --help              output usage information
    -V, --version           output the version number
    --prefix <prefix>       Prefex to place at the front of each SQS queue name [qdone_]
    --fail-suffix <suffix>  Suffix to append to each queue to generate fail queue name [_failed]
    --region <region>       AWS region for Queues

  AWS SQS Authentication:

    You must provide one of:
    1) On AWS instances: an IAM role that allows the appropriate SQS calls
    2) A credentials file (~/.aws/credentials) containing a [default] section with appropriate keys
    3) Both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY as environment variables

  Examples:

    $ qdone enqueue process-image "/usr/bin/php /var/myapp/process-image.php http://imgur.com/some-cool-cat.jpg"
    $ qdone worker process-image

## Worker

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

