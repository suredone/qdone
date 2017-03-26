
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
  { name: 'prefix', type: String, defaultValue: 'qdone_', description: 'Prefex to place at the front of each SQS queue name [default: qdone_]' },
  { name: 'fail-suffix', type: String, defaultValue: '_failed', description: 'Suffix to append to each queue to generate fail queue name [default: _failed]' },
  { name: 'region', type: String, defaultValue: 'us-east-1', description: 'AWS region for Queues [default: us-east-1]' },
  { name: 'version', alias: 'V', type: Boolean, description: 'Show version number' },
  { name: 'help', type: Boolean, description: 'Print full help message.' }
]

exports.enqueue = function enqueue (argv, globalOptions) {
  const usageSections = [
    { content: 'usage: qdone [options] enqueue <queue> <command>', raw: true },
    {
      content: [
        { summary: 'API call count: 2 [ + 3 ]' },
        { summary: '- one call to resolve the queue name' },
        { summary: '- one call to enqueue the command' },
        { summary: '- three extra calls if the queue does not exist yet' }
      ]
    },
    { content: 'Options', raw: true },
    { optionList: globalOptionDefinitions },
    awsUsageHeader, awsUsageBody
  ]
  const optionDefinitions = [
    { name: 'help', type: Boolean, description: 'Print full help message.' }
  ]
  debug('enqueue argv', argv)

  // Parse command and options
  try {
    var options = commandLineArgs(optionDefinitions, {argv})
    debug('enqueue options', options)
    options = Object.assign(globalOptions, options)
    debug('enqueue merged options', options)
    if (options.help) return Promise.resolve(console.log(getUsage(usageSections)))
    if (argv.length !== 2) throw new UsageError('enqueue requires both <queue> and <command> arguments')
    var [ queue, command ] = argv
    debug('queue', queue, 'command', command)
  } catch (e) {
    console.log(getUsage(usageSections.filter(s => !s.long)))
    return Promise.reject(e)
  }

  // Load module now, after AWS global load
  const enqueue = require('./enqueue')

  // Normal (non batch) enqueue
  return enqueue
    .enqueue(queue, command, options)
    .then(function (result) {
      debug('enqueue returned', result)
      console.error(chalk.blue('Enqueued job ') + result.MessageId)
      return result
    })
}

exports.enqueueBatch = function enqueueBatch (argv, globalOptions) {
  const usageSections = [
    { content: 'usage: qdone [options] enqueue-batch <file...>', raw: true },
    { content: '<file...> can be one ore more filenames or - for stdin' },
    {
      content: [
        { summary: 'API call count: q + ceil(c/10) [ + 3n ]' },
        { summary: '- where q is the number of unique queue names in the batch' },
        { summary: '- and c is the number of commands in the batch' },
        { summary: '- and n is the number of queues that do not exist yet' }
      ]
    },
    { content: 'Options', raw: true },
    { optionList: globalOptionDefinitions },
    awsUsageHeader, awsUsageBody
  ]
  const optionDefinitions = [
    { name: 'help', type: Boolean, description: 'Print full help message.' }
  ]
  debug('enqueue-batch argv', argv)

  // Parse command and options
  let files
  try {
    var options = commandLineArgs(optionDefinitions, {argv})
    debug('enqueue-batch options', options)
    options = Object.assign(globalOptions, options)
    debug('enqueue-batch merged options', options)
    if (options.help) return Promise.resolve(console.log(getUsage(usageSections)))
    if (argv.length === 0) throw new UsageError('enqueue-batch requres one or more <file> arguments')
    files = argv.map(f => f === '-' ? process.stdin : fs.createReadStream(f))
    debug('filenames', argv)
  } catch (e) {
    console.log(getUsage(usageSections.filter(s => !s.long)))
    return Promise.reject(new UsageError(e))
  }

  // Load module now, after AWS global load
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
        console.error(chalk.blue('Enqueued ') + result + chalk.blue(' jobs'))
      })
  })
}

exports.worker = function worker (argv, globalOptions) {

}

exports.root = function root (originalArgv) {
  const validCommands = [ null, 'enqueue', 'enqueue-batch', 'worker' ]
  const usageSections = [
    { content: 'qdone - Command line job queue for SQS', raw: true, long: true },
    { content: 'usage: qdone <options> <command>', raw: true },
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
    var options = commandLineArgs(globalOptionDefinitions, {argv: originalArgv})
    var { command, argv } = commandLineCommands(validCommands, originalArgv)
    debug('command', command)
    debug('options', options)
  } catch (err) {
    console.log(getUsage(usageSections.filter(s => !s.long)))
    return Promise.reject(err)
  }

  // Root command
  if (command === null) {
    if (options.version) return Promise.resolve(console.log(packageJson.version))
    else if (options.help) return Promise.resolve(console.log(getUsage(usageSections)))
    else console.log(getUsage(usageSections.filter(s => !s.long)))
    return Promise.resolve()
  }

  // Setup AWS
  debug('loading aws-sdk')
  const AWS = require('aws-sdk')
  AWS.config.setPromisesDependency(require('q').Promise)
  AWS.config.update({region: options.region})
  debug('loaded')

  // Run child commands
  if (command === 'enqueue') {
    return exports.enqueue(argv, options, options)
  } else if (command === 'enqueue-batch') {
    return exports.enqueueBatch(argv, options, options)
  } else if (command === 'worker') {
    return exports.worker(argv, options, options)
  }
}

exports.run = function run (argv) {
  debug('run', argv)
  return exports
    .root(argv)
    .catch(function (err) {
      if (err.code === 'AccessDenied') console.log(getUsage([awsUsageHeader, awsUsageBody]))
      console.error(chalk.red.bold(err))
      throw err
    })
}

debug('loaded')
