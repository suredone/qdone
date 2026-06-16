/**
 * Implementation for the worker that pulls jobs from queue and executes them.
 */

import {
  ChangeMessageVisibilityCommand,
  ReceiveMessageCommand,
  DeleteMessageCommand,
  QueueDoesNotExist
} from '@aws-sdk/client-sqs'
import { exec, execFile } from 'child_process' // node:child_process
import treeKill from 'tree-kill'
import chalk from 'chalk'
import Debug from 'debug'

import { dedupSuccessfullyProcessed } from './dedup.js'
import { normalizeQueueName, getQnameUrlPairs } from './qrlCache.js'
import { getOptionsWithDefaults } from './defaults.js'
import { cheapIdleCheck } from './idleQueues.js'
import { getSQSClient } from './sqs.js'
import { checkCommand } from './commandPolicy.js'
import { reportEvent } from './sentry.js'

const debug = Debug('qdone:worker')

// Global flag for shutdown request
let shutdownRequested = false

export function requestShutdown () {
  shutdownRequested = true
}

//
// Actually run the subprocess job
//
export async function executeJob (job, qname, qrl, opt) {
  debug('executeJob', job)
  const cmd = 'nice ' + job.Body
  if (opt.archive) {
    await getSQSClient().send(new DeleteMessageCommand({
      QueueUrl: qrl,
      ReceiptHandle: job.ReceiptHandle
    }))
    console.log(cmd)
    return { noJobs: 0, jobsSucceeded: 1, jobsFailed: 0 }
  }

  // Command allowlist policy. 'off' (default) is a pure no-op and skips this
  // entirely. 'audit' logs/alerts on violations but still runs via the existing
  // shell path. 'enforce' rejects violations and runs validated commands via
  // execFile (no shell). NOTE: this is the only path that executes a job body as
  // a process; src/scheduler/jobExecutor.js delegates to a caller-supplied
  // callback and does not exec bodies — if that ever changes, apply checkCommand
  // there too.
  let argv = null
  if (opt.commandPolicy && opt.commandPolicy !== 'off') {
    const check = checkCommand(job.Body, opt)
    if (check.misconfig) {
      // Config error (missing/invalid allowlist): fail OPEN to avoid an outage,
      // but alert loudly so the misconfiguration is caught.
      const misconfig = {
        event: 'COMMAND_POLICY_MISCONFIG',
        timestamp: new Date(),
        policy: opt.commandPolicy,
        queue: qname,
        messageId: job.MessageId,
        reason: check.misconfig
      }
      console.log(JSON.stringify(misconfig))
      await reportEvent(opt, 'error', 'qdone command policy misconfigured (failing open)', { commandPolicy: misconfig })
    } else if (!check.ok) {
      const violation = {
        event: 'COMMAND_POLICY_VIOLATION',
        timestamp: new Date(),
        policy: opt.commandPolicy,
        queue: qname,
        messageId: job.MessageId,
        command: job.Body,
        reason: check.reason
      }
      console.log(JSON.stringify(violation))
      await reportEvent(opt, 'error', 'qdone command policy violation', { commandPolicy: violation })
      if (opt.commandPolicy === 'enforce') {
        if (opt.verbose) console.error(chalk.red('  REJECTED by command policy: ') + check.reason)
        // Do NOT delete and do NOT execute. Returning jobsFailed leaves the
        // message for SQS redrive → DLQ (after dlqAfter receives), preserving a
        // false positive for inspection and bounding a malicious message.
        return { noJobs: 0, jobsSucceeded: 0, jobsFailed: 1 }
      }
      // audit: fall through, argv stays null → existing shell path
    } else if (opt.commandPolicy === 'enforce') {
      argv = check.argv // validated → run without a shell
    }
  }

  if (opt.verbose) console.error(chalk.blue('  Executing job command:'), cmd)

  const jobStart = new Date()
  let visibilityTimeout = 30 // this should be the queue timeout
  let timeoutExtender

  async function extendTimeout () {
    debug('extendTimeout')
    const maxJobRun = 12 * 60 * 60
    const jobRunTime = ((new Date()) - jobStart) / 1000
    // Double every time, up to max
    visibilityTimeout = Math.min(visibilityTimeout * 2, maxJobRun - jobRunTime, opt.killAfter - jobRunTime)
    if (opt.verbose) {
      console.error(
        chalk.blue('  Ran for ') + jobRunTime +
        chalk.blue(' seconds, requesting another ') + visibilityTimeout +
        chalk.blue(' seconds')
      )
    }

    try {
      const result = await getSQSClient().send(new ChangeMessageVisibilityCommand({
        QueueUrl: qrl,
        ReceiptHandle: job.ReceiptHandle,
        VisibilityTimeout: visibilityTimeout
      }))
      debug('ChangeMessageVisibility.then returned', result)
      if (
        jobRunTime + visibilityTimeout >= maxJobRun ||
        jobRunTime + visibilityTimeout >= opt.killAfter
      ) {
        if (opt.verbose) console.error(chalk.yellow('  warning: this is our last time extension'))
      } else {
        // Extend when we get 50% of the way to timeout
        timeoutExtender = setTimeout(extendTimeout, visibilityTimeout * 1000 * 0.5)
      }
    } catch (err) {
      debug('changeMessageVisibility.catch returned', err)
      // Rejection means we're ouuta time, whatever, let the job die
      if (opt.verbose) console.error(chalk.red('  failed to extend job: ') + err)
    }
  }

  // Extend when we get 50% of the way to timeout
  timeoutExtender = setTimeout(extendTimeout, visibilityTimeout * 1000 * 0.5)
  debug('timeout', visibilityTimeout * 1000 * 0.5)

  // NOTE: Due to #25 we cannot rely on child_process.exec's timeout option because
  // it does not seem to work for child processes of the shell, so we'll create our
  // own timeout and use tree-kill to catch all of the child processes.

  let child, sigKillTimeout
  function killTree () {
    debug('killTree', child.pid)
    treeKill(child.pid, 'SIGTERM')
    setTimeout(function () {
      sigKillTimeout = treeKill(child.pid, 'SIGKILL')
    }, 1000)
  }
  const treeKiller = setTimeout(killTree, opt.killAfter * 1000)
  debug({ treeKiller: opt.killAfter * 1000, date: Date.now() })

  // Build environment with SQS message attributes for child process
  const env = {
    ...process.env,
    QDONE_QUEUE_NAME: qname,
    SQS_MESSAGE_ID: job.MessageId || '',
    SQS_RECEIVE_COUNT: job.Attributes?.ApproximateReceiveCount || '1',
    SQS_SENT_TIMESTAMP: job.Attributes?.SentTimestamp || '',
    SQS_FIRST_RECEIVE_TIMESTAMP: job.Attributes?.ApproximateFirstReceiveTimestamp || '',
    SQS_MESSAGE_GROUP_ID: job.Attributes?.MessageGroupId || ''
  }

  try {
    // Success path for job execution
    const { stdout, stderr } = await new Promise(function (resolve, reject) {
      const cb = function (err, stdout, stderr) {
        if (err) {
          err.stdout = stdout
          err.stderr = stderr
          reject(err)
        } else resolve({ stdout, stderr })
      }
      // enforce + valid → run `nice <argv...>` with no shell. Otherwise (off /
      // audit / enforce-misconfig) → unchanged shell exec of `nice <body>`.
      child = argv
        ? execFile('nice', argv, { env }, cb)
        : exec(cmd, { env }, cb)
    })

    debug('exec.then', Date.now())
    clearTimeout(timeoutExtender)
    clearTimeout(treeKiller)
    clearTimeout(sigKillTimeout)
    if (opt.verbose) {
      console.error(chalk.green('  SUCCESS'))
      if (stdout) console.error(chalk.blue('  stdout: ') + stdout)
      if (stderr) console.error(chalk.blue('  stderr: ') + stderr)
      console.error(chalk.blue('  cleaning up (removing job) ...'))
    }
    await getSQSClient().send(new DeleteMessageCommand({
      QueueUrl: qrl,
      ReceiptHandle: job.ReceiptHandle
    }))

    if (opt.verbose) {
      console.error(chalk.blue('  done'))
      console.error()
    }

    // Let dedup system know we processed it
    await dedupSuccessfullyProcessed(job, opt)

    return { noJobs: 0, jobsSucceeded: 1, jobsFailed: 0 }
  } catch (err) {
    // Fail path for job execution
    debug('exec.catch')
    clearTimeout(timeoutExtender)
    clearTimeout(treeKiller)
    clearTimeout(sigKillTimeout)
    if (opt.verbose) {
      const { code, signal, stdout, stderr } = err
      console.error(chalk.red('  FAILED'))
      if (code) console.error(chalk.blue('  code  : ') + code)
      if (signal) console.error(chalk.blue('  signal: ') + signal)
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
        exitCode: err.code || err.code || undefined,
        killSignal: err.signal || undefined,
        stderr: err.stderr,
        stdout: err.stdout,
        errorMessage: err.toString().split('\n').slice(1).join('\n').trim() || undefined
      }))
    }
    return { noJobs: 0, jobsSucceeded: 0, jobsFailed: 1 }
  }
}

//
// Pull work off of a single queue
//
export async function pollForJobs (qname, qrl, opt) {
  debug('pollForJobs')
  const params = {
    AttributeNames: ['All'],
    MaxNumberOfMessages: 1,
    MessageAttributeNames: ['All'],
    QueueUrl: qrl,
    VisibilityTimeout: 30,
    WaitTimeSeconds: opt.waitTime
  }
  const response = await getSQSClient().send(new ReceiveMessageCommand(params))
  debug('sqs.receiveMessage.then', response)
  if (shutdownRequested) return { noJobs: 0, jobsSucceeded: 0, jobsFailed: 0 }
  if (response.Messages) {
    const job = response.Messages[0]
    if (opt.verbose) console.error(chalk.blue('  Found job ' + job.MessageId))
    return executeJob(job, qname, qrl, opt)
  } else {
    return { noJobs: 1, jobsSucceeded: 0, jobsFailed: 0 }
  }
}

//
// Resolve queues for listening loop listen
//
export async function listen (queues, options) {
  const opt = getOptionsWithDefaults(options)
  debug({ opt, options })
  // Function to listen to all queues in order
  async function oneRound (queues) {
    const stats = { noJobs: 0, jobsSucceeded: 0, jobsFailed: 0 }
    for (const { qname, qrl } of queues) {
      if (shutdownRequested) return stats
      if (opt.verbose) {
        console.error(
          chalk.blue('Looking for work on ') +
          qname.slice(opt.prefix.length) +
          chalk.blue(' (' + qrl + ')')
        )
      }
      try {
        // Aggregate the results
        const { noJobs, jobsSucceeded, jobsFailed } = await pollForJobs(qname, qrl, opt)
        stats.noJobs += noJobs
        stats.jobsFailed += jobsFailed
        stats.jobsSucceeded += jobsSucceeded
      } catch (e) {
        if (e instanceof QueueDoesNotExist) {
          if (opt.verbose) {
            console.error(
              chalk.yellow('Warning: Queue ') +
              qname.slice(opt.prefix.length) +
              chalk.yellow(' does not exist.')
            )
          }
        } else {
          throw e
        }
      }
    }
    return stats
  }

  // Start processing
  if (opt.verbose) console.error(chalk.blue('Resolving queues: ') + queues.join(' '))
  const qnames = queues.map(queue => normalizeQueueName(queue, opt))
  const pairs = await getQnameUrlPairs(qnames, opt)

  // Figure out which pairs are active
  const activePairs = []
  if (opt.activeOnly) {
    debug({ pairsBeforeCheck: pairs })
    await Promise.all(pairs.map(async pair => {
      const { result: { idle } } = await cheapIdleCheck(pair.qname, pair.qrl, opt)
      if (!idle) activePairs.push(pair)
    }))
  }

  // Finished resolving
  debug('getQnameUrlPairs.then')
  if (opt.verbose) {
    console.error(chalk.blue('  done'))
    console.error()
  }

  // Figure out which queues we want to listen on, choosing between active and
  // all, filtering out failed queues if the user wants that
  const selectedPairs = (opt.activeOnly ? activePairs : pairs)
    .filter(({ qname }) => {
      const suf = opt.failSuffix + (opt.fifo ? '.fifo' : '')
      const isFailQueue = qname.slice(-suf.length) === suf
      const shouldInclude = opt.includeFailed ? true : !isFailQueue
      return shouldInclude
    })

  // But only if we have queues to listen on
  if (selectedPairs.length) {
    if (opt.verbose) {
      console.error(chalk.blue('Listening to queues (in this order):'))
      console.error(selectedPairs.map(({ qname, qrl }) =>
        '  ' + qname.slice(opt.prefix.length) + chalk.blue(' - ' + qrl)
      ).join('\n'))
      console.error()
    }
    return oneRound(selectedPairs)
  }

  // Otherwise, let caller know
  return 'noQueues'
}

debug('loaded')
