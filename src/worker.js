
const Q = require('q')
const childProcess = require('child_process')
const debug = require('debug')('qdone:worker')
const chalk = require('chalk')
const treeKill = require('tree-kill')
const qrlCache = require('./qrlCache')
const cheapIdleCheck = require('./idleQueues').cheapIdleCheck
const AWS = require('aws-sdk')
var shutdownRequested = false

exports.requestShutdown = function requestShutdown () {
  shutdownRequested = true
}

//
// Actually run the subprocess job
//
function executeJob (job, qname, qrl, options) {
  debug('executeJob', job)
  const cmd = 'nice ' + job.Body
  if (options.verbose) console.error(chalk.blue('  Executing job command:'), cmd)

  var jobStart = new Date()
  var visibilityTimeout = 30 // this should be the queue timeout
  var timeoutExtender

  function extendTimeout () {
    debug('extendTimeout')
    const maxJobRun = 12 * 60 * 60
    const jobRunTime = ((new Date()) - jobStart) / 1000
    // Double every time, up to max
    visibilityTimeout = Math.min(visibilityTimeout * 2, maxJobRun - jobRunTime, options['kill-after'] - jobRunTime)
    if (options.verbose) {
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
          if (options.verbose) console.error(chalk.yellow('  warning: this is our last time extension'))
        } else {
          // Extend when we get 50% of the way to timeout
          timeoutExtender = setTimeout(extendTimeout, visibilityTimeout * 1000 * 0.5)
        }
      })
      .catch(function (err) {
        debug('changeMessageVisibility.catch returned', err)
        // Rejection means we're ouuta time, whatever, let the job die
        if (options.verbose) console.error(chalk.red('  failed to extend job: ') + err)
      })
  }

  // Extend when we get 50% of the way to timeout
  timeoutExtender = setTimeout(extendTimeout, visibilityTimeout * 1000 * 0.5)
  debug('timeout', visibilityTimeout * 1000 * 0.5)

  // NOTE: Due to #25 we cannot rely on child_process.exec's timeout option because
  // it does not seem to work for child processes of the shell, so we'll create our
  // own timeout and use tree-kill to catch all of the child processes.

  let child
  function killTree () {
    debug('killTree', child.pid)
    treeKill(child.pid, 'SIGTERM')
    setTimeout(function () {
      treeKill(child.pid, 'SIGKILL')
    }, 1000)
  }
  const treeKiller = setTimeout(killTree, options['kill-after'] * 1000)
  debug({ treeKiller: options['kill-after'] * 1000, date: Date.now() })

  const promise = new Promise(function (resolve, reject) {
    child = childProcess.exec(cmd, function (err, stdout, stderr) {
      if (err) reject(err, stdout, stderr)
      else resolve(stdout, stderr)
    })
  })

  return promise
    // Q.nfcall(childProcess.exec, cmd, {timeout: options['kill-after'] * 1000})
    .then(function (stdout, stderr) {
      debug('childProcess.exec.then', Date.now())
      clearTimeout(timeoutExtender)
      clearTimeout(treeKiller)
      if (options.verbose) {
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
          if (options.verbose) {
            console.error(chalk.blue('  done'))
            console.error()
          }
          return Promise.resolve({ noJobs: 0, jobsSucceeded: 1, jobsFailed: 0 })
        })
    })
    .catch((err, stdout, stderr) => {
      debug('childProcess.exec.catch')
      clearTimeout(timeoutExtender)
      clearTimeout(treeKiller)
      if (options.verbose) {
        console.error(chalk.red('  FAILED'))
        if (err.code) console.error(chalk.blue('  code  : ') + err.code)
        if (err.signal) console.error(chalk.blue('  signal: ') + err.signal)
        if (stdout) console.error(chalk.blue('  stdout: ') + stdout)
        if (stderr) console.error(chalk.blue('  stderr: ') + stderr)
        console.error(chalk.blue('  error : ') + err)
      } else {
        // Production error logging
        console.log(JSON.stringify({
          event: 'JOB_FAILED',
          timestamp: new Date(),
          job: job.MessageId,
          command: job.Body,
          exitCode: err.code || undefined,
          killSignal: err.signal || undefined,
          stderr,
          stdout,
          errorMessage: err.toString().split('\n').slice(1).join('\n').trim() || undefined
        }))
      }
      return Promise.resolve({ noJobs: 0, jobsSucceeded: 0, jobsFailed: 1 })
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
      if (shutdownRequested) return Promise.resolve({ noJobs: 0, jobsSucceeded: 0, jobsFailed: 0 })
      if (response.Messages) {
        const job = response.Messages[0]
        if (options.verbose) console.error(chalk.blue('  Found job ' + job.MessageId))
        return executeJob(job, qname, qrl, options)
      } else {
        return Promise.resolve({ noJobs: 1, jobsSucceeded: 0, jobsFailed: 0 })
      }
    })
}

//
// Resolve queues for listening loop listen
//
exports.listen = function listen (queues, options) {
  if (options.verbose) console.error(chalk.blue('Resolving queues: ') + queues.join(' '))
  const qnames = queues.map(function (queue) { return options.prefix + qrlCache.normalizeQueueName(queue, options) })
  debug({ hello: '?' })
  return qrlCache
    .getQnameUrlPairs(qnames, options)
    .then(function (entries) {
      // If user only wants active queues, run a cheap idle check
      if (options['active-only']) {
        debug({ entiresBeforeCheck: entries })
        return Promise.all(entries.map(entry =>
          cheapIdleCheck(entry.qname, entry.qrl, options)
            .then(result =>
              Promise.resolve(
                Object.assign(entry, { idle: result.idle })
              )
            )
        ))
      } else {
        return entries
      }
    })
    .then(function (entries) {
      if (options['active-only']) {
        // Filter out idle queues
        return entries.filter(entry => entry && entry.idle !== true)
      } else {
        return entries
      }
    })
    .then(function (entries) {
      debug('qrlCache.getQnameUrlPairs.then')
      if (options.verbose) {
        console.error(chalk.blue('  done'))
        console.error()
      }

      // Don't listen to fail queues... unless user wants to
      entries = entries
        .filter(function (entry) {
          const suf = options['fail-suffix'] + (options.fifo ? '.fifo' : '')
          return options['include-failed'] ? true : entry.qname.slice(-suf.length) !== suf
        })

      // Listen to all queues once
      function oneRound () {
        var result = Q()
        entries.forEach(function (entry) {
          debug('entries.forEach.funtion')
          result = result.then((soFar = { noJobs: 0, jobsSucceeded: 0, jobsFailed: 0 }) => {
            debug('soFar', soFar)
            // Don't poll the next queue if shutdown was requested
            if (shutdownRequested) return Promise.resolve(soFar)
            if (options.verbose) {
              console.error(
                chalk.blue('Looking for work on ') +
                entry.qname.slice(options.prefix.length) +
                chalk.blue(' (' + entry.qrl + ')')
              )
            }
            // Aggregate the results
            return pollForJobs(entry.qname, entry.qrl, options)
              .then(({ noJobs, jobsSucceeded, jobsFailed }) => ({
                noJobs: soFar.noJobs + noJobs,
                jobsSucceeded: soFar.jobsSucceeded + jobsSucceeded,
                jobsFailed: soFar.jobsFailed + jobsFailed
              }))
          })
        })
        return result
      }

      // But only if we have queues to listen on
      if (entries.length) {
        if (options.verbose) {
          console.error(chalk.blue('Listening to queues (in this order):'))
          console.error(entries.map(function (e) {
            return '  ' + e.qname.slice(options.prefix.length) + chalk.blue(' - ' + e.qrl)
          }).join('\n'))
          console.error()
        }
        return oneRound()
      }

      // Otherwise, let caller know
      return Promise.resolve('noQueues')
    })
}

debug('loaded')
