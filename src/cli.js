const fs = require('fs')
const readline = require('readline')
const program = require('commander')
const debug = require('debug')('qdone')
const chalk = require('chalk')
const AWS = require('aws-sdk')
AWS.config.setPromisesDependency(require('q').Promise)

function configureAWS (options) {
  AWS.config.update({region: options.region || 'us-east-1'})
}

function awsDocs () {
  console.log('  AWS SQS Authentication:')
  console.log()
  console.log('    You must provide ONE of:')
  console.log('    1) On AWS instances: an IAM role that allows the appropriate SQS calls')
  console.log('    2) A credentials file (~/.aws/credentials) containing a [default] section with appropriate keys')
  console.log('    3) Both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY as environment variables')
  console.log()
}

program
  .version('0.0.1')
  .option('--prefix <prefix>', 'Prefex to place at the front of each SQS queue name [qdone_]', 'qdone_')
  .option('--fail-suffix <suffix>', 'Suffix to append to each queue to generate fail queue name [_failed]', '_failed')
  .option('--region <region>', 'AWS region for Queues')

program
  .command('enqueue <queue> <command>')
  .description('Enqueue command on the specified queue')
  .action(function (queue, command, options) {
    configureAWS(program)
    const enqueue = require('./enqueue')

    // Normal (non batch) enqueue
    enqueue
      .enqueue(queue, command, options, program)
      .then(function (result) {
        debug('enqueue returned', result)
        console.error(chalk.blue('Enqueued job ') + result.MessageId)
        process.exit(0)
      })
      .fail(function (err) {
        console.error(chalk.red(err))
        process.exit(1)
      })
  })

program
  .command('enqueue-batch [file]')
  .description('Read a list of (queue, command) pairs, one per line, from [file...] or stdin.')
  .action((file, options) => {
    configureAWS(program)
    const enqueue = require('./enqueue')
    const pairs = []

    // Construct (queue, command) pairs from input
    debug('file', file)
    const input = readline.createInterface({
      input: (file && file !== '-') ? fs.createReadStream(file) : process.stdin
    })

    input.on('line', line => {
      const parts = line.split(/\s+/)
      const queue = parts[0]
      const command = line.slice(queue.length).trim()
      pairs.push({queue, command})
    })

    input.on('close', _ => {
      debug(pairs)
      enqueue
        .enqueueBatch(pairs, options, program)
        .then(function (result) {
          debug('enqueueBatch returned', result)
          console.error(chalk.blue('Enqueued ') + result + chalk.blue(' jobs'))
          process.exit(0)
        })
        .fail(function (err) {
          console.error(chalk.red(err))
          throw err
        })
    })
  })

program
  .command('worker <queue...>')
  .description('Listen for work on one or more queues')
  .option('-k, --kill-after <seconds>', 'Kill job after this many seconds [30]', value => {
    const parsed = parseInt(value)
    const killAfterMax = 12 * 60 * 60
    if (Number.isNaN(parsed) || parsed < 0 || parsed > killAfterMax) {
      console.error(chalk.red('  error: --kill-after <seconds> should be a number from 0 to ' + killAfterMax))
      process.exit(1)
    }
    return parsed
  }, 30)
  .option('-w, --wait-time <seconds>', 'Listen this long on each queue before moving to next [10]', value => {
    const parsed = parseInt(value)
    const waitTimeMax = 20
    debug('parsed', parsed)
    if (Number.isNaN(parsed) || parsed < 0 || parsed > waitTimeMax) {
      console.error(chalk.red('  error: --wait-time <seconds> should be a number from 0 to ' + waitTimeMax))
      process.exit(1)
    }
    return parsed
  }, 10)
  .option('--always-resolve', 'Always resolve queue names that end in \'*\'. This can result\n                           in more SQS calls, but allows you to listen to queues that\n                           do not exist yet.', false)
  .option('--include-failed', 'When using \'*\' do not ignore fail queues.', false)
  .option('--drain', 'Run until no more work is found and quit. NOTE: if used with\n                           --wait-time 0, this option will not drain queues.', false)
  .action(function (queue, options) {
    configureAWS(program)
    const worker = require('./worker')
    options.killAfter = Number.isNaN(options.killAfter) ? 30 : options.killAfter
    options.waitTime = Number.isNaN(options.waitTime) ? 10 : options.waitTime
    debug('killAfter', options.killAfter, queue)
    debug('waitTime', options.waitTime)
    function workLoop () {
      return worker
        .listen(queue, options, program)
        .then(function (result) {
          debug('listen returned', result)
          // Doing the work loop out here forces queue resolution to happen every time
          if (options.alwaysResolve && !options.drain) return workLoop()
          process.exit(0)
        })
        .catch(function (err) {
          console.error(chalk.red(err))
          console.error()
          if (err.code === 'AccessDenied') awsDocs()
          process.exit(1)
        })
    }
    workLoop()
  })
  .on('--help', function () {
    console.log('  Details:')
    console.log()
    console.log('    If a queue name ends with the * (wildcard) character, worker will listen on all')
    console.log('    queues that match the name up-to the wildcard. Place arguments like this inside')
    console.log('    quotes to keep the shell from globbing local files.')
    console.log()
    console.log('  Examples:')
    console.log()
    console.log('    $ qdone worker process-images     # listen on a single queue')
    console.log('    $ qdone worker one two three      # listen on multiple queues')
    console.log('    $ qdone worker "process-images-*" # listen to both process-images-png and')
    console.log('                                      # process-images-jpeg if those queues exist')
    console.log()
  })

/*
program
  .command('status [queue...]')
  .description('Report status of the overall system or the given queue(s)')
  .action(function (queue, options) {
    debug('queue', queue)
    configureAWS(options)
    debug('args', options.args)
  })
*/

program
  .on('--help', function () {
    awsDocs()
    console.log('  Examples:')
    console.log()
    console.log('    $ qdone enqueue process-image "/usr/bin/php /var/myapp/process-image.php http://imgur.com/some-cool-cat.jpg"')
    console.log('    $ qdone worker process-image')
    console.log()
  })

program.command('*')
  .action(function () {
    program.outputHelp()
    process.exit(1)
  })

function run (argv) {
  if (argv.length === 2) {
    program.outputHelp()
    process.exit(1)
  } else {
    program.parse(argv)
  }
}

module.exports.run = run
