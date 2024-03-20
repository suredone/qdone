/**
 * Command line interface implementation
 */
import { createReadStream, openSync } from 'node:fs'
import { createInterface } from 'node:readline'
import { createRequire } from 'module'
import getUsage from 'command-line-usage'
import commandLineCommands from 'command-line-commands'
import commandLineArgs from 'command-line-args'
import Debug from 'debug'
import chalk from 'chalk'

import { QueueDoesNotExist } from '@aws-sdk/client-sqs'
import { defaults, setupAWS, setupVerbose, getOptionsWithDefaults } from './defaults.js'
import { shutdownCache } from './cache.js'
import { withSentry } from './sentry.js'

const debug = Debug('qdone:cli')
const require = createRequire(import.meta.url)
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
  { name: 'prefix', type: String, description: `Prefix to place at the front of each SQS queue name [default: ${defaults.prefix}]` },
  { name: 'fail-suffix', type: String, description: `Suffix to append to each queue to generate fail queue name [default: ${defaults.failSuffix}]` },
  { name: 'region', type: String, description: `AWS region for Queues [default: ${defaults.region}]` },
  { name: 'quiet', alias: 'q', type: Boolean, description: 'Turn on production logging. Automatically set if stderr is not a tty.' },
  { name: 'verbose', alias: 'v', type: Boolean, description: 'Turn on verbose output. Automatically set if stderr is a tty.' },
  { name: 'version', alias: 'V', type: Boolean, description: 'Show version number' },
  { name: 'cache-uri', type: String, description: 'URL to caching cluster. Only redis://... currently supported.' },
  { name: 'cache-prefix', type: String, description: `Prefix for all keys in cache. [default: ${defaults.cachePrefix}]` },
  { name: 'cache-ttl-seconds', type: Number, description: `Number of seconds to cache GetQueueAttributes calls. [default: ${defaults.cacheTtlSeconds}]` },
  { name: 'help', type: Boolean, description: 'Print full help message.' },
  { name: 'external-dedup', type: Boolean, description: 'Moves deduplication from SQS to qdone external cache, allowing a longer deduplication window and alternate semantics.' },
  { name: 'dedup-period', type: Number, description: 'Number of seconds (counting from the first enqueue) to prevent a duplicate message from being sent. Resets after a message with that deduplication id has been successfully processed. Minumum 360 seconds.' },
  { name: 'dedup-stats', type: Boolean, description: 'Keeps statistics on dedup keys. Disabled by default. Increases load on redis.' },
  { name: 'sentry-dsn', type: String, description: 'Optional Sentry DSN to track unhandled errors.' }
]

const enqueueOptionDefinitions = [
  { name: 'fifo', alias: 'f', type: Boolean, description: 'Create new queues as FIFOs' },
  { name: 'group-id', alias: 'g', type: String, description: 'FIFO Group ID to use for all messages enqueued in current command. Defaults to a string unique to this invocation.' },
  { name: 'group-id-per-message', type: Boolean, description: 'Use a unique Group ID for every message, even messages in the same batch.' },
  { name: 'deduplication-id', type: String, description: 'A Message Deduplication ID to give SQS when sending a message. Use this option if you are managing retries outside of qdone, and make sure the ID is the same for each retry in the deduplication window. Defaults to a string unique to this invocation.' },
  { name: 'message-retention-period', type: Number, description: `Number of seconds to retain jobs (up to 14 days). [default: ${defaults.messageRetentionPeriod}]` },
  { name: 'delay', type: Number, description: 'Delays delivery of the enqueued message by the given number of seconds (up to 900 seconds, or 15 minutes). Defaults to immediate delivery (no delay).' },
  { name: 'fail-delay', type: Number, description: 'Delays delivery of all messages on this queue by the given number of seconds (up to 900 seconds, or 15 minutes). Only takes effect if this queue is created during this enqueue operation. Defaults to immediate delivery (no delay).' },
  { name: 'dlq', type: Boolean, description: 'Send messages from the failed queue to a DLQ.' },
  { name: 'dql-suffix', type: String, description: `Suffix to append to each queue to generate DLQ name [default: ${defaults.dlqSuffix}]` },
  { name: 'dql-after', type: String, description: `Drives message to the DLQ after this many failures in the failed queue. [default: ${defaults.dlqAfter}]` },
  { name: 'tag', type: String, multiple: true, description: 'Adds an AWS tag to queue creation. Use the format Key=Value. Can specify multiple times.' }
]

export async function enqueue (argv, testHook) {
  const optionDefinitions = [].concat(enqueueOptionDefinitions, globalOptionDefinitions)
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
  let options, queue, command
  try {
    options = commandLineArgs(optionDefinitions, { argv, partial: true })
    setupVerbose(options)
    debug('enqueue options', options)
    if (options.help) return Promise.resolve(console.log(getUsage(usageSections)))
    if (!options._unknown || options._unknown.length !== 2) throw new UsageError('enqueue requires both <queue> and <command> arguments')
    queue = options._unknown[0]
    command = options._unknown[1]
    debug('queue', queue, 'command', command)
  } catch (err) {
    console.log(getUsage(usageSections.filter(s => !s.long)))
    throw err
  }

  // Process tags
  if (options.tag && options.tag.length) {
    options.tags = {}
    for (const input of options.tag) {
      debug({ input })
      if (input.indexOf('=') === -1) throw new UsageError('Tags must be separated with the "=" character.')
      const [key, ...rest] = input.split('=')
      const value = rest.join('=')
      debug({ input, key, rest, value, tags: options.tags })
      options.tags[key] = value
    }
  }

  // Load module after AWS global load
  setupAWS(options)
  const { enqueue: enqueueOriginal } = await import('./enqueue.js')
  const enqueue = testHook || enqueueOriginal

  // Normal (non batch) enqueue
  const opt = getOptionsWithDefaults(options)
  const result = (
    await withSentry(async () => enqueue(queue, command, opt), opt)
  )
  debug('enqueue returned', result)
  if (options.verbose) console.error(chalk.blue('Enqueued job ') + result.MessageId)
  return result
}

const monitorOptionDefinitions = [
  { name: 'save', alias: 's', type: Boolean, description: 'Saves data to CloudWatch' }
]

export async function monitor (argv) {
  const optionDefinitions = [].concat(monitorOptionDefinitions, globalOptionDefinitions)
  const usageSections = [
    { content: 'usage: qdone monitor <queuePattern> ', raw: true },
    { content: 'Options', raw: true },
    { optionList: optionDefinitions },
    { content: 'SQS API Call Complexity', raw: true, long: true },
    {
      content: [
        { count: '1 + N', summary: 'one call to resolve the queue names (potentially more calls if there are pages)\none call per queue to get attributes' }
      ],
      long: true
    },
    awsUsageHeader, awsUsageBody
  ]
  debug('monitor argv', argv)

  // Parse command and options
  let options, queue
  try {
    options = commandLineArgs(optionDefinitions, { argv, partial: true })
    setupVerbose(options)
    debug('enqueue options', options)
    if (options.help) return Promise.resolve(console.log(getUsage(usageSections)))
    if (!options._unknown || options._unknown.length !== 1) throw new UsageError('monitor requires the <queuePattern> argument')
    queue = options._unknown[0]
    debug('queue', queue)
  } catch (e) {
    console.log(getUsage(usageSections.filter(s => !s.long)))
    return Promise.reject(e)
  }

  // Load module after AWS global load
  setupAWS(options)
  const { getAggregateData } = await import('./monitor.js')
  const { putAggregateData } = await import('./cloudWatch.js')
  const data = await getAggregateData(queue)
  console.log(data)
  if (options.save) {
    process.stderr.write('Saving to CloudWatch...')
    await putAggregateData(data)
    process.stderr.write('done\n')
  }
  return data
}

export async function loadBatchFile (filename) {
  const file = filename === '-' ? process.stdin : createReadStream(filename, { fd: openSync(filename, 'r') })
  const pairs = []
  await new Promise((resolve, reject) => {
    debug('file', file.name || 'stdin')
    // Construct (queue, command) pairs from input
    const input = createInterface({ input: file })
    input.on('line', line => {
      const parts = line.split(/\s+/)
      const queue = parts[0]
      const command = line.slice(queue.length).trim()
      pairs.push({ queue, command })
    })
    input.on('error', reject)
    input.on('close', resolve)
  })
  return pairs
}

export async function loadBatchFiles (filenames) {
  const results = await Promise.all(filenames.map(loadBatchFile))
  const pairs = results.flat()
  return pairs
}

export async function enqueueBatch (argv, testHook) {
  const optionDefinitions = [].concat(enqueueOptionDefinitions, globalOptionDefinitions)
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
  let filenames, options
  try {
    options = commandLineArgs(optionDefinitions, { argv, partial: true })
    setupVerbose(options)
    debug('enqueue-batch options', options)
    if (options.help) return Promise.resolve(console.log(getUsage(usageSections)))
    if (!options._unknown || options._unknown.length === 0) throw new UsageError('enqueue-batch requres one or more <file> arguments')
    debug('filenames', options._unknown)
    filenames = options._unknown
  } catch (err) {
    console.log(getUsage(usageSections.filter(s => !s.long)))
    throw err
  }

  // Load module after AWS global load
  setupAWS(options)
  const { enqueueBatch: enqueueBatchOriginal } = await import('./enqueue.js')
  const enqueueBatch = testHook || enqueueBatchOriginal

  // Load data and enqueue it
  const pairs = await loadBatchFiles(filenames)
  debug('pairs', pairs)

  // Normal (non batch) enqueue
  const opt = getOptionsWithDefaults(options)
  const result = (
    await withSentry(async () => enqueueBatch(pairs, opt), opt)
  )
  debug('enqueueBatch returned', result)
  if (options.verbose) console.error(chalk.blue('Enqueued ') + result + chalk.blue(' jobs'))
}

export async function worker (argv, testHook) {
  const optionDefinitions = [
    { name: 'kill-after', alias: 'k', type: Number, defaultValue: 30, description: 'Kill job after this many seconds [default: 30]' },
    { name: 'wait-time', alias: 'w', type: Number, defaultValue: 20, description: 'Listen at most this long on each queue [default: 20]' },
    { name: 'include-failed', type: Boolean, description: 'When using \'*\' do not ignore fail queues.' },
    { name: 'active-only', type: Boolean, description: 'Listen only to queues with pending messages.' },
    { name: 'drain', type: Boolean, description: 'Run until no more work is found and quit. NOTE: if used with  --wait-time 0, this option will not drain queues.' },
    { name: 'archive', type: Boolean, description: 'Does not run jobs, just prints commands to stdout. Use this flag for draining a queue and recording the commands that were in it.' },
    { name: 'fifo', alias: 'f', type: Boolean, description: 'Automatically adds .fifo to queue names. Only listens to fifo queues when using \'*\'.' }
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
        { context: 'while listening', count: 'n + (1 per n*w)', desc: 'w: --wait-time in seconds\nn: number of queues' },
        { context: 'while job running', count: 'log(t/30) + 1', desc: 't: total job run time in seconds' }
      ],
      long: true
    },
    awsUsageHeader, awsUsageBody
  ]
  debug('enqueue-batch argv', argv)

  // Parse command and options
  let queues, options
  try {
    options = commandLineArgs(optionDefinitions, { argv, partial: true })
    setupVerbose(options)
    debug('worker options', options)
    if (options.help) return Promise.resolve(console.log(getUsage(usageSections)))
    if (!options._unknown || options._unknown.length === 0) throw new UsageError('worker requres one or more <queue> arguments')
    if (options.drain && options['wait-time'] === 0) throw new UsageError('cannot use --drain with --wait-time 0 (SQS limitation)')
    queues = options._unknown
    debug('queues', queues)
  } catch (err) {
    console.log(getUsage(usageSections.filter(s => !s.long)))
    throw err
  }

  // Load module after AWS global load
  setupAWS(options)
  const { listen: originalListen, requestShutdown } = await import('./worker.js')
  const listen = testHook || originalListen

  let jobCount = 0
  let jobsSucceeded = 0
  let jobsFailed = 0
  let shutdownRequested = false

  function handleShutdown () {
    // Second signal forces shutdown
    if (shutdownRequested) {
      if (options.verbose) console.error(chalk.red('Recieved multiple kill signals, shutting down immediately.'))
      process.kill(-process.pid, 'SIGKILL')
    }
    shutdownRequested = true
    requestShutdown()
    if (options.verbose) {
      console.error(chalk.yellow('Shutdown requested. Will stop when current job is done or a second signal is recieved.'))
      if (process.stdout.isTTY) {
        console.error(chalk.yellow('NOTE: Interactive shells often signal whole process group so your child may exit.'))
      }
    }
  }
  process.on('SIGINT', handleShutdown)
  process.on('SIGTERM', handleShutdown)

  async function workLoop () {
    if (shutdownRequested) {
      if (options.verbose) console.error(chalk.blue('Shutting down as requested.'))
      return Promise.resolve()
    }
    // const result = await listen(queues, options)
    const opt = getOptionsWithDefaults(options)
    const result = (
      await withSentry(async () => listen(queues, opt), opt)
    )
    debug('listen returned', result)

    // Handle delay in the case we don't have any queues
    if (result === 'noQueues') {
      const roundDelay = Math.max(1000, options['wait-time'] * 1000)
      if (options.verbose) console.error(chalk.yellow('No queues to listen on!'))
      if (options.drain) {
        console.error(chalk.blue('Shutting down because we are in drain mode and no work is available.'))
        return Promise.resolve()
      }
      console.error(chalk.yellow('Retrying in ' + (roundDelay / 1000) + 's'))
      const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms))
      return delay(roundDelay).then(workLoop)
    }

    const ranJob = (result.jobsSucceeded + result.jobsFailed) > 0
    jobCount += result.jobsSucceeded + result.jobsFailed
    jobsFailed += result.jobsFailed
    jobsSucceeded += result.jobsSucceeded
    // Draining continues to listen as long as there is work
    if (options.drain) {
      if (ranJob) return workLoop()
      if (options.verbose) {
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
  }

  return workLoop()
}

export async function idleQueues (argv, testHook) {
  const optionDefinitions = [
    { name: 'idle-for', alias: 'o', type: Number, defaultValue: defaults.idleFor, description: `Minutes of inactivity after which a queue is considered idle. [default: ${defaults.idleFor}]` },
    { name: 'delete', type: Boolean, description: 'Delete the queue if it is idle. The fail queue also must be idle unless you use --unpair.' },
    { name: 'unpair', type: Boolean, description: 'Treat queues and their fail queues as independent. By default they are treated as a unit.' },
    { name: 'include-failed', type: Boolean, description: 'When using \'*\' do not ignore fail queues. This option only applies if you use --unpair. Otherwise, queues and fail queues are treated as a unit.' }
  ].concat(globalOptionDefinitions)

  const usageSections = [
    { content: 'usage: qdone idle-queues [options] <queue...>', raw: true },
    { content: 'Options', raw: true },
    { optionList: optionDefinitions },
    { content: 'SQS API Call Complexity', raw: true, long: true },
    {
      content: [
        { count: '1 + q + i', desc: 'q: number of queues in pattern\ni: number of idle queues' },
        { context: 'with --delete options', count: '1 + q + 3i', desc: 'q: number of queues in pattern\ni: number of idle queues' },
        { context: 'with --unpair option', count: '1 + q', desc: 'q: number of queues in pattern' },
        { context: 'with --unpair and --delete options', count: '1 + q + i', desc: 'q: number of queues in pattern\ni: number of idle queues' },
        { desc: 'NOTE: the --unpair option not cheaper if you include fail queues, because it doubles q.' }
      ],
      long: true
    },
    { content: 'CloudWatch API Call Complexity', raw: true, long: true },
    {
      content: [
        { count: 'min: 0 (if queue and fail queue have waiting messages)\nmax: 12q\nexpected (approximate observed): 0.5q + 12i', desc: 'q: number of queues in pattern\ni: number of idle queues' },
        { context: 'with --unpair option', count: 'min: 0 (if queue has waiting messages)\nmax: 6q\nexpected (approximate observed): q + 6i', desc: 'q: number of queues in pattern\ni: number of idle queues' },
        { desc: 'NOTE: the --unpair option not cheaper if you include fail queues, because it doubles q.' }
      ],
      long: true
    },
    awsUsageHeader, awsUsageBody
  ]
  debug('idleQueues argv', argv)

  // Parse command and options
  let queues, options
  try {
    options = commandLineArgs(optionDefinitions, { argv, partial: true })
    setupVerbose(options)
    debug('idleQueues options', options)
    if (options.help) return Promise.resolve(console.log(getUsage(usageSections)))
    if (!options._unknown || options._unknown.length === 0) throw new UsageError('idle-queues requres one or more <queue> arguments')
    if (options['include-failed'] && !options.unpair) throw new UsageError('--include-failed should be used with --unpair')
    if (options['idle-for'] < 5) throw new UsageError('--idle-for must be at least 5 minutes (CloudWatch limitation)')
    queues = options._unknown
    debug('queues', queues)
  } catch (e) {
    console.log(getUsage(usageSections.filter(s => !s.long)))
    return Promise.reject(e)
  }

  // Load module after AWS global load
  setupAWS(options)
  const { idleQueues: idleQueuesOriginal } = await import('./idleQueues.js')
  const idleQueues = testHook || idleQueuesOriginal
  const opt = getOptionsWithDefaults(options)
  try {
    const result = (
      await withSentry(async () => idleQueues(queues, opt), opt)
    )
    debug('idleQueues returned', result)
    if (result === 'noQueues') return Promise.resolve()
    const callsSQS = result.map(a => a.apiCalls.SQS).reduce((a, b) => a + b, 0)
    const callsCloudWatch = result.map(a => a.apiCalls.CloudWatch).reduce((a, b) => a + b, 0)
    if (options.verbose) console.error(chalk.blue('Used ') + callsSQS + chalk.blue(' SQS and ') + callsCloudWatch + chalk.blue(' CloudWatch API calls.'))

    // Print idle queues to stdout
    result.filter(a => a.idle).map(a => a.queue).forEach(q => console.log(q))
    return result
  } catch (err) {
    if (err instanceof QueueDoesNotExist) {
      console.error(chalk.yellow('This error can occur when you run this command immediately after deleting a queue. Wait 60 seconds and try again.'))
    }
    throw err
  }
}

export async function root (originalArgv, testHook) {
  const validCommands = [null, 'enqueue', 'enqueue-batch', 'worker', 'idle-queues', 'monitor']
  const usageSections = [
    { content: 'qdone - Command line job queue for SQS', raw: true, long: true },
    { content: 'usage: qdone [options] <command>', raw: true },
    { content: 'Commands', raw: true },
    {
      content: [
        { name: 'enqueue', summary: 'Enqueue a single command' },
        { name: 'enqueue-batch', summary: 'Enqueue multiple commands from stdin or a file' },
        { name: 'worker', summary: 'Execute work on one or more queues' },
        { name: 'idle-queues', summary: 'Write a list of idle queues to stdout' },
        { name: 'monitor', summary: 'Monitor multiple queues at once' }
      ]
    },
    { content: 'Global Options', raw: true },
    { optionList: globalOptionDefinitions },
    awsUsageHeader, awsUsageBody
  ]

  // Parse command and options
  let command, argv
  try {
    const parsed = commandLineCommands(validCommands, originalArgv)
    command = parsed.command
    argv = parsed.argv
    debug('command', command)

    // Root command
    if (command === null) {
      const options = commandLineArgs(globalOptionDefinitions, { argv: originalArgv })
      setupVerbose(options)
      debug('options', options)
      if (options.version) return console.log(packageJson.version)
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
    return enqueue(argv, testHook)
  } else if (command === 'enqueue-batch') {
    return enqueueBatch(argv, testHook)
  } else if (command === 'worker') {
    return worker(argv, testHook)
  } else if (command === 'idle-queues') {
    return idleQueues(argv, testHook)
  } else if (command === 'monitor') {
    return monitor(argv, testHook)
  }
}

export async function run (argv, testHook) {
  debug('run', argv)
  try {
    await root(argv, testHook)
    // If cache actually is active, it will keep our program from exiting
    // until we disconnect the cache client
    shutdownCache()
  } catch (err) {
    if (err.Code === 'AccessDenied') console.log(getUsage([awsUsageHeader, awsUsageBody]))
    console.error(chalk.red.bold(err))
    console.error(err.stack.slice(err.stack.indexOf('\n') + 1))
    throw err
  }
}

debug('loaded')
