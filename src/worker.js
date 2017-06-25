
const Q = require('q')
const childProcess = require('child_process')
const debug = require('debug')('qdone:worker')
const chalk = require('chalk')
const qrlCache = require('./qrlCache')
const AWS = require('aws-sdk')

//
// Actually run the subprocess job
//
function executeJob (job, qname, qrl, options) {
  debug('executeJob', job)
  const cmd = 'nice ' + job.Body
  if (!options.quiet) console.error(chalk.blue('  Executing job command:'), cmd)

  var jobStart = new Date()
  var visibilityTimeout = 30 // this should be the queue timeout
  var timeoutExtender

  function extendTimeout () {
    debug('extendTimeout')
    const maxJobRun = 12 * 60 * 60
    const jobRunTime = ((new Date()) - jobStart) / 1000
    // Double every time, up to max
    visibilityTimeout = Math.min(visibilityTimeout * 2, maxJobRun - jobRunTime, options['kill-after'] - jobRunTime)
    if (!options.quiet) {
      console.error(
        chalk.blue('  Ran for ') + jobRunTime +
        chalk.blue(' seconds, requesting another ') + visibilityTimeout +
        chalk.blue(' seconds')
      )
    }
    const sqs = new AWS.SQS()
    sqs
      .changeMessageVisibility({
        QueueUrl: qrl,
        ReceiptHandle: job.ReceiptHandle,
        VisibilityTimeout: visibilityTimeout
      })
      .promise()
      .then(function (result) {
        debug('changeMessageVisibility.then returned', result)
        if (
          jobRunTime + visibilityTimeout >= maxJobRun ||
          jobRunTime + visibilityTimeout >= options['kill-after']
        ) {
          if (!options.quiet) console.error(chalk.yellow('  warning: this is our last time extension'))
        } else {
          // Extend when we get 50% of the way to timeout
          timeoutExtender = setTimeout(extendTimeout, visibilityTimeout * 1000 * 0.5)
        }
      })
      .fail(function (err) {
        debug('changeMessageVisibility.fail returned', err)
        // Rejection means we're ouuta time, whatever, let the job die
        if (!options.quiet) console.error(chalk.red('  failed to extend job: ') + err)
      })
  }

  // Extend when we get 50% of the way to timeout
  timeoutExtender = setTimeout(extendTimeout, visibilityTimeout * 1000 * 0.5)

  return Q
    .nfcall(childProcess.exec, cmd, {timeout: options['kill-after'] * 1000})
    .then(function (stdout, stderr) {
      debug('childProcess.exec.then')
      clearTimeout(timeoutExtender)
      if (!options.quiet) {
        console.error(chalk.green('  SUCCESS'))
        if (stdout) console.error(chalk.blue('  stdout: ') + stdout)
        if (stderr) console.error(chalk.blue('  stderr: ') + stderr)
        console.error(chalk.blue('  cleaning up (removing job) ...'))
      }
      const sqs = new AWS.SQS()
      return sqs
        .deleteMessage({
          QueueUrl: qrl,
          ReceiptHandle: job.ReceiptHandle
        })
        .promise()
        .then(function () {
          if (!options.quiet) {
            console.error(chalk.blue('  done'))
            console.error()
          }
        })
    })
    .fail((err, stdout, stderr) => {
      debug('childProcess.exec.fail')
      clearTimeout(timeoutExtender)
      if (!options.quiet) {
        console.error(chalk.red('  FAILED'))
        if (err.code) console.error(chalk.blue('  code  : ') + err.code)
        if (err.signal) console.error(chalk.blue('  signal: ') + err.signal)
        if (stdout) console.error(chalk.blue('  stdout: ') + stdout)
        if (stderr) console.error(chalk.blue('  stderr: ') + stderr)
        console.error(chalk.blue('  error : ') + err)
      } else {
        // Production error logging
        console.error(JSON.stringify({
          status: 'FAILED',
          job: job.MessageId,
          command: job.Body,
          exitCode: err.code || undefined,
          killSignal: err.signal || undefined,
          stderr,
          stdout,
          errorMessage: err.toString().split('\n').slice(1).join('\n').trim() || undefined
        }))
      }
    })
}

//
// Pull work off of a single queue
//
function pollForJobs (qname, qrl, options) {
  debug('pollForJobs')
  const params = {
    AttributeNames: ['All'],
    MaxNumberOfMessages: 1,
    MessageAttributeNames: ['All'],
    QueueUrl: qrl,
    VisibilityTimeout: 30,
    WaitTimeSeconds: options['wait-time']
  }
  const sqs = new AWS.SQS()
  return sqs
    // .receiveMessage(params, function (err, response) { if (err) throw err })
    .receiveMessage(params)
    .promise()
    .then(function (response) {
      debug('sqs.receiveMessage.then', response)
      if (response.Messages) {
        const job = response.Messages[0]
        if (!options.quiet) console.error(chalk.blue('  Found job ' + job.MessageId))
        return executeJob(job, qname, qrl, options)
      }
    })
}

//
// Resolve queues for listening loop listen
//
exports.listen = function listen (queues, options) {
  if (!options.quiet) console.error(chalk.blue('Resolving queues: ') + queues.join(' '))
  const qnames = queues.map(function (queue) { return options.prefix + queue })
  return qrlCache
    .getQnameUrlPairs(qnames, options)
    .then(function (entries) {
      debug('qrlCache.getQnameUrlPairs.then')
      if (!options.quiet) {
        console.error(chalk.blue('  done'))
        console.error()
      }

      // Don't listen to fail queues... unless user wants to
      entries = entries
        .filter(function (entry) {
          const suf = options['fail-suffix']
          return options['include-failed'] ? true : entry.qname.slice(-suf.length) !== suf
        })

      // Listen sequentially
      function workLoop () {
        var result = Q()
        entries.forEach(function (entry) {
          debug('entries.forEach.funtion')
          result = result.then(function (soFar) {
            debug('soFar', soFar)
            if (!options.quiet) {
              console.error(
                chalk.blue('Looking for work on ') +
                entry.qname.slice(options.prefix.length) +
                chalk.blue(' (' + entry.qrl + ')')
              )
            }
            return pollForJobs(entry.qname, entry.qrl, options)
          })
        })

        // Do the work loop in here to NOT resolve queues every time
        return result.then(result => (options['always-resolve'] || options.drain) ? result : workLoop())
      }

      // But only if we have queues to listen on
      if (entries.length) {
        if (!options.quiet) {
          console.error(chalk.blue('Listening to queues (in this order):'))
          console.error(entries.map(function (e) {
            return '  ' + e.qname.slice(options.prefix.length) + chalk.blue(' - ' + e.qrl)
          }).join('\n'))
          console.error()
        }
        return workLoop()
      }
    })
}

debug('loaded')
