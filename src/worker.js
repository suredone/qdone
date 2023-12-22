/**
 * Implementation for the worker that pulls jobs from queue and executes them.
 */

import {
  ChangeMessageVisibilityCommand,
  ReceiveMessageCommand,
  DeleteMessageCommand
} from '@aws-sdk/client-sqs'
import { exec } from 'node:child_process'
import treeKill from 'tree-kill'
import chalk from 'chalk'
import Debug from 'debug'

import { normalizeQueueName, getQnameUrlPairs } from './qrlCache.js'
import { getOptionsWithDefaults } from './defaults.js'
import { cheapIdleCheck } from './idleQueues.js'
import { getSQSClient } from './sqs.js'

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

  try {
    // Success path for job execution
    const { stdout, stderr } = await new Promise(function (resolve, reject) {
      child = exec(cmd, function (err, stdout, stderr) {
        if (err) {
          err.stdout = stdout
          err.stderr = stderr
          reject(err)
        } else resolve({ stdout, stderr })
      })
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
      // Aggregate the results
      const { noJobs, jobsSucceeded, jobsFailed } = await pollForJobs(qname, qrl, opt)
      stats.noJobs += noJobs
      stats.jobsFailed += jobsFailed
      stats.jobsSucceeded += jobsSucceeded
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
      const { idle } = await cheapIdleCheck(pair.qname, pair.qrl, opt)
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
