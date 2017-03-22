
const Q = require('q')
const childProcess = require('child_process')
const debug = require('debug')('qdone:worker')
const chalk = require('chalk')
const qrlCache = require('./qrlCache')
const SQS = require('aws-sdk/clients/sqs')
const sqs = new SQS()

//
// Actually run the subprocess job
//
function executeJob (job, qname, qrl, killAfter) {
  debug('executeJob')
  const cmd = 'nice ' + job.Body
  console.error(chalk.blue('  Executing job command:'), cmd)

  var jobStart = new Date()
  var visibilityTimeout = 30 // this should be the queue timeout
  var timeoutExtender

  function extendTimeout () {
    debug('extendTimeout')
    const maxJobRun = 12 * 60 * 60
    const jobRunTime = ((new Date()) - jobStart) / 1000
    // Double every time, up to max
    visibilityTimeout = Math.min(visibilityTimeout * 2, maxJobRun - jobRunTime, killAfter - jobRunTime)
    console.error(
      chalk.blue('  Ran for ') + jobRunTime +
      chalk.blue(' seconds, requesting another ') + visibilityTimeout +
      chalk.blue(' seconds')
    )
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
          jobRunTime + visibilityTimeout >= killAfter
        ) {
          console.error(chalk.yellow('  warning: this is our last time extension'))
        } else {
          // Extend when we get 50% of the way to timeout
          timeoutExtender = setTimeout(extendTimeout, visibilityTimeout * 1000 * 0.5)
        }
      })
      .fail(function (err) {
        debug('changeMessageVisibility.fail returned', err)
        // Rejection means we're ouuta time, whatever, let the job die
        console.error(chalk.red('  failed to extend job: ') + err)
      })
  }

  // Extend when we get 50% of the way to timeout
  timeoutExtender = setTimeout(extendTimeout, visibilityTimeout * 1000 * 0.5)

  return Q
    .nfcall(childProcess.exec, cmd, {timeout: killAfter * 1000})
    .then(function (stdout, stderr) {
      debug('childProcess.exec.then')
      clearTimeout(timeoutExtender)
      console.error(chalk.green('  SUCCESS'))
      stdout && console.error(chalk.blue('  stdout: ') + stdout)
      stderr && console.error(chalk.blue('  stderr: ') + stderr)
      console.error(chalk.blue('  cleaning up (removing job) ...'))
      return sqs
        .deleteMessage({
          QueueUrl: qrl,
          ReceiptHandle: job.ReceiptHandle
        })
        .promise()
        .then(function () {
          console.error(chalk.blue('  done'))
          console.error()
        })
    })
    .fail(function (err, stdout, stderr) {
      debug('childProcess.exec.fail')
      clearTimeout(timeoutExtender)
      console.error(chalk.red('  FAILED'))
      err.code && console.error(chalk.blue('  code  : ') + err.code)
      err.signal && console.error(chalk.blue('  signal: ') + err.signal)
      stdout && console.error(chalk.blue('  stdout: ') + stdout)
      stderr && console.error(chalk.blue('  stderr: ') + stderr)
      console.error(chalk.blue('  error : ') + err)
    })
}

//
// Pull work off of a single queue
//
function pollForJobs (qname, qrl, waitTime, killAfter) {
  debug('pollForJobs')
  const params = {
    AttributeNames: ['All'],
    MaxNumberOfMessages: 1,
    MessageAttributeNames: ['All'],
    QueueUrl: qrl,
    VisibilityTimeout: 30,
    WaitTimeSeconds: waitTime
  }
  return sqs
    // .receiveMessage(params, function (err, response) { if (err) throw err })
    .receiveMessage(params)
    .promise()
    .then(function (response) {
      debug('sqs.receiveMessage.then', response)
      if (response.Messages) {
        const job = response.Messages[0]
        console.error(chalk.blue('  Found job ' + job.MessageId))
        return executeJob(job, qname, qrl, killAfter)
      }
    })
}

//
// Resolve queues for listening loop listen
//
exports.listen = function listen (queues, options, prefix) {
  console.error(chalk.blue('Resolving queues:'))
  const qnames = queues.map(function (queue) { return prefix + queue })
  return qrlCache
    .getQnameUrlPairs(qnames, prefix)
    .then(function (entries) {
      debug('qrlCache.getQnameUrlPairs.then')
      console.error(chalk.blue('  done'))
      console.error()

      // Listen sequentially
      function workLoop () {
        var result = Q()
        entries.forEach(function (entry) {
          debug('entries.forEach.funtion')
          result = result.then(function (soFar) {
            debug('soFar', soFar)
            console.error(
              chalk.blue('Looking for work on ') +
              entry.qname.slice(prefix.length) +
              chalk.blue(' (' + entry.qrl + ')')
            )
            return pollForJobs(entry.qname, entry.qrl, options.waitTime, options.killAfter)
          })
        })
        return result.then(function (result) {
          // Do the work loop in here to NOT resolve queues every time
          debug(options.alwaysResolve)
          return options.alwaysResolve ? result : workLoop()
        })
      }

      // But only if we have queues to listen on
      if (entries.length) {
        console.error(chalk.blue('Listening to queues (in this order):'))
        console.error(entries.map(function (e) {
          return '  ' + e.qname.slice(prefix.length) + chalk.blue(' - ' + e.qrl)
        }).join('\n'))
        console.error()
        return workLoop()
      }
    })
}

debug('loaded')
