
const debug = require('debug')('qdone:cli')
const Q = require('q')
const fs = require('fs')
const readline = require('readline')
const chalk = require('chalk')
const commandLineCommands = require('command-line-commands')
const commandLineArgs = require('command-line-args')
const getUsage = require('command-line-usage')
const packageJson = require('../package.json')

class UsageError extends Error {}

const awsUsageHeader = { content: 'AWS SQS Authentication', raw: true, long: true }
const awsUsageBody = {
  content: [
    { summary: 'You must provide ONE of:' },
    { summary: '1) On AWS instances: an IAM role that allows the appropriate SQS calls' },
    { summary: '2) A credentials file (~/.aws/credentials) containing a [default] section with appropriate keys' },
    { summary: '3) Both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY as environment variables' }
  ],
  long: true
}

const globalOptionDefinitions = [
  { name: 'prefix', type: String, defaultValue: 'qdone_', description: 'Prefix to place at the front of each SQS queue name [default: qdone_]' },
  { name: 'fail-suffix', type: String, defaultValue: '_failed', description: 'Suffix to append to each queue to generate fail queue name [default: _failed]' },
  { name: 'region', type: String, defaultValue: 'us-east-1', description: 'AWS region for Queues [default: us-east-1]' },
  { name: 'quiet', alias: 'q', type: Boolean, defaultValue: !process.stdout.isTTY, description: 'Less verbose output suitible for production logging. Automatically set if stdout is not a tty.' },
  { name: 'version', alias: 'V', type: Boolean, description: 'Show version number' },
  { name: 'help', type: Boolean, description: 'Print full help message.' }
]

function setupAWS (options) {
  debug('loading aws-sdk')
  const AWS = require('aws-sdk')
  AWS.config.setPromisesDependency(Q.Promise)
  AWS.config.update({region: options.region})
  debug('loaded')
}

exports.enqueue = function enqueue (argv) {
  const optionDefinitions = [].concat(globalOptionDefinitions)
  const usageSections = [
    { content: 'usage: qdone enqueue [options] <queue> <command>', raw: true },
    { content: 'Options', raw: true },
    { optionList: optionDefinitions },
    { content: 'SQS API Call Complexity', raw: true, long: true },
    {
      content: [
        { count: '2 [ + 3 ]', summary: 'one call to resolve the queue name\none call to enqueue the command\nthree extra calls if queue does not exist yet' }
      ],
      long: true
    },
    awsUsageHeader, awsUsageBody
  ]
  debug('enqueue argv', argv)

  // Parse command and options
  try {
    var options = commandLineArgs(optionDefinitions, {argv, partial: true})
    debug('enqueue options', options)
    if (options.help) return Promise.resolve(console.log(getUsage(usageSections)))
    if (!options._unknown || options._unknown.length !== 2) throw new UsageError('enqueue requires both <queue> and <command> arguments')
    var [ queue, command ] = options._unknown
    debug('queue', queue, 'command', command)
  } catch (e) {
    console.log(getUsage(usageSections.filter(s => !s.long)))
    return Promise.reject(e)
  }

  // Load module after AWS global load
  setupAWS(options)
  const enqueue = require('./enqueue')

  // Normal (non batch) enqueue
  return enqueue
    .enqueue(queue, command, options)
    .then(function (result) {
      debug('enqueue returned', result)
      if (!options.quiet) console.error(chalk.blue('Enqueued job ') + result.MessageId)
      return result
    })
}

exports.enqueueBatch = function enqueueBatch (argv) {
  const optionDefinitions = [].concat(globalOptionDefinitions)
  const usageSections = [
    { content: 'usage: qdone enqueue-batch [options] <file...>', raw: true },
    { content: '<file...> can be one ore more filenames or - for stdin' },
    { content: 'Options', raw: true },
    { optionList: optionDefinitions },
    { content: 'SQS API Call Complexity', raw: true, long: true },
    {
      content: [
        { count: 'q + ceil(c/10) [ + 3n ]', summary: 'q: number of unique queue names in the batch\nc: number of commands in the batch\nn: number of queues that do not exist yet' }
      ],
      long: true
    },
    awsUsageHeader, awsUsageBody
  ]
  debug('enqueue-batch argv', argv)

  // Parse command and options
  let files
  try {
    var options = commandLineArgs(optionDefinitions, {argv, partial: true})
    debug('enqueue-batch options', options)
    if (options.help) return Promise.resolve(console.log(getUsage(usageSections)))
    if (!options._unknown || options._unknown.length === 0) throw new UsageError('enqueue-batch requres one or more <file> arguments')
    debug('filenames', options._unknown)
    files = options._unknown.map(f => f === '-' ? process.stdin : fs.createReadStream(f, {fd: fs.openSync(f, 'r')}))
  } catch (err) {
    console.log(getUsage(usageSections.filter(s => !s.long)))
    return Promise.reject(err)
  }

  // Load module after AWS global load
  setupAWS(options)
  const enqueue = require('./enqueue')
  const pairs = []

  // Load data and enqueue it
  return Promise.all(
    files.map(function (file) {
      // Construct (queue, command) pairs from input
      debug('file', file.name || 'stdin')
      const input = readline.createInterface({input: file})
      const deferred = Q.defer()
      input.on('line', line => {
        const parts = line.split(/\s+/)
        const queue = parts[0]
        const command = line.slice(queue.length).trim()
        pairs.push({queue, command})
      })
      input.on('error', deferred.reject)
      input.on('close', deferred.resolve)
      return deferred.promise
    })
  )
  .then(function () {
    debug('pairs', pairs)
    return enqueue
      .enqueueBatch(pairs, options)
      .then(function (result) {
        debug('enqueueBatch returned', result)
        if (!options.quiet) console.error(chalk.blue('Enqueued ') + result + chalk.blue(' jobs'))
      })
  })
}

exports.worker = function worker (argv) {
  const optionDefinitions = [
    { name: 'kill-after', alias: 'k', type: Number, defaultValue: 30, description: 'Kill job after this many seconds [default: 30]' },
    { name: 'wait-time', alias: 'w', type: Number, defaultValue: 20, description: 'Listen at most this long on each queue [default: 20]' },
    { name: 'include-failed', type: Boolean, description: 'When using \'*\' do not ignore fail queues.' },
    { name: 'drain', type: Boolean, description: 'Run until no more work is found and quit. NOTE: if used with  --wait-time 0, this option will not drain queues.' }
  ].concat(globalOptionDefinitions)

  const usageSections = [
    { content: 'usage: qdone worker [options] <queue...>', raw: true },
    { content: '<queue...> one or more queue names to listen on for jobs' },
    { content: 'If a queue name ends with the * (wildcard) character, worker will listen on all queues that match the name up-to the wildcard. Place arguments like this inside quotes to keep the shell from globbing local files.' },
    { content: 'Options', raw: true },
    { optionList: optionDefinitions },
    { content: 'SQS API Call Complexity', raw: true, long: true },
    {
      content: [
        { contex: 'while listening', count: 'n + (1 per n*w)', desc: 'w: --wait-time in seconds\nn: number of queues' },
        { contex: 'while job running', count: 'log(t/30) + 1', desc: 't: total job run time in seconds' }
      ],
      long: true
    },
    awsUsageHeader, awsUsageBody
  ]
  debug('enqueue-batch argv', argv)

  // Parse command and options
  let queues
  try {
    var options = commandLineArgs(optionDefinitions, {argv, partial: true})
    debug('worker options', options)
    if (options.help) return Promise.resolve(console.log(getUsage(usageSections)))
    if (!options._unknown || options._unknown.length === 0) throw new UsageError('worker requres one or more <queue> arguments')
    if (options.drain && options['wait-time'] === 0) throw new UsageError('cannot use --drain with --wait-time 0 (SQS limitation)')
    queues = options._unknown
    debug('queues', queues)
  } catch (err) {
    console.log(getUsage(usageSections.filter(s => !s.long)))
    return Promise.reject(err)
  }

  // Load module after AWS global load
  setupAWS(options)
  const worker = require('./worker')

  var jobCount = 0
  var jobsSucceeded = 0
  var jobsFailed = 0
  var shutdownRequested = false

  function handleShutdown () {
    // Second signal forces shutdown
    if (shutdownRequested) {
      if (!options.quiet) console.error(chalk.red('Recieved multiple kill signals, shutting down immediately.'))
      process.kill(-process.pid, 'SIGKILL')
    }
    shutdownRequested = true
    if (!options.quiet) {
      console.error(chalk.yellow('Shutdown requested. Will stop when current job is done or a second signal is recieved.'))
      if (process.stdout.isTTY) {
        console.error(chalk.yellow('NOTE: Interactive shells often signal whole process group so your child may exit.'))
      }
    }
  }
  process.on('SIGINT', handleShutdown)
  process.on('SIGTERM', handleShutdown)

  function workLoop () {
    if (shutdownRequested) {
      if (!options.quiet) console.error(chalk.blue('Shutting down as requested.'))
      return Promise.resolve()
    }
    return worker
      .listen(queues, options)
      .then(function (result) {
        debug('listen returned', result)

        // Handle delay in the case we don't have any queues
        if (result === 'noQueues') {
          const roundDelay = Math.max(1000, options['wait-time'] * 1000)
          if (!options.quiet) console.error(chalk.yellow('No queues to listen on!'))
          if (options.drain) {
            console.error(chalk.blue('Shutting down because we are in drain mode and no work is available.'))
            return Promise.resolve()
          }
          console.error(chalk.yellow('Retrying in ' + (roundDelay / 1000) + 's'))
          return Q.delay(roundDelay).then(workLoop)
        }

        const ranJob = (result.jobsSucceeded + result.jobsFailed) > 0
        jobCount += result.jobsSucceeded + result.jobsFailed
        jobsFailed += result.jobsFailed
        jobsSucceeded += result.jobsSucceeded
        // Draining continues to listen as long as there is work
        if (options.drain) {
          if (ranJob) return workLoop()
          if (!options.quiet) {
            console.error(chalk.blue('Ran ') + jobCount + chalk.blue(' jobs: ') + jobsSucceeded + chalk.blue(' succeeded ') + jobsFailed + chalk.blue(' failed'))
          }
          // return Promise.resolve(jobCount)
        } else {
          // If we're not draining, loop forever
          // We can go immediately if we just ran a job
          if (ranJob) return workLoop()
          // Otherwise, we could do backoff logic here to slow down requests when
          // work is not happening (at the expense of latency)
          // But we won't do that now.
          return workLoop()
        }
      })
  }
  return workLoop()
}

exports.root = function root (originalArgv) {
  const validCommands = [ null, 'enqueue', 'enqueue-batch', 'worker' ]
  const usageSections = [
    { content: 'qdone - Command line job queue for SQS', raw: true, long: true },
    { content: 'usage: qdone [options] <command>', raw: true },
    { content: 'Commands', raw: true },
    { content: [
        { name: 'enqueue', summary: 'Enqueue a single command' },
        { name: 'enqueue-batch', summary: 'Enqueue multiple commands from stdin or a file' },
        { name: 'worker', summary: 'Execute work on one or more queues' }
    ] },
    { content: 'Global Options', raw: true },
    { optionList: globalOptionDefinitions },
    awsUsageHeader, awsUsageBody
  ]

  // Parse command and options
  try {
    var { command, argv } = commandLineCommands(validCommands, originalArgv)
    debug('command', command)

    // Root command
    if (command === null) {
      const options = commandLineArgs(globalOptionDefinitions, {argv: originalArgv})
      debug('options', options)
      if (options.version) return Promise.resolve(console.log(packageJson.version))
      else if (options.help) return Promise.resolve(console.log(getUsage(usageSections)))
      else console.log(getUsage(usageSections.filter(s => !s.long)))
      return Promise.resolve()
    }
  } catch (err) {
    console.log(getUsage(usageSections.filter(s => !s.long)))
    return Promise.reject(err)
  }

  // Run child commands
  if (command === 'enqueue') {
    return exports.enqueue(argv)
  } else if (command === 'enqueue-batch') {
    return exports.enqueueBatch(argv)
  } else if (command === 'worker') {
    return exports.worker(argv)
  }
}

exports.run = function run (argv) {
  debug('run', argv)
  return exports
    .root(argv)
    .catch(function (err) {
      if (err.code === 'AccessDenied') console.log(getUsage([awsUsageHeader, awsUsageBody]))
      console.error(chalk.red.bold(err))
      console.error(err.stack.slice(err.stack.indexOf('\n') + 1))
      throw err
    })
}

debug('loaded')
