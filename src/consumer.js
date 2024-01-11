/**
 * Consumer implementation.
 */

import {
  ChangeMessageVisibilityCommand,
  ReceiveMessageCommand,
  DeleteMessageCommand
} from '@aws-sdk/client-sqs'
import chalk from 'chalk'
import Debug from 'debug'

import { normalizeQueueName, getQnameUrlPairs } from './qrlCache.js'
import { cheapIdleCheck } from './idleQueues.js'
import { getOptionsWithDefaults } from './defaults.js'
import { getSQSClient } from './sqs.js'

//
// Throwing an instance of this Error allows the processMessages callback to
// refuse a message which then gets immediately returned to the queue.
//
// This has the side effect of throtting the queue since it stops polling on
// the queue until the next queue resolution in processMessages.
//
// This is useful for implementing schedulers on top of qdone, for example, to
// look at the queue name and decide whether to take on a new message.
//
export class DoNotProcess extends Error {}

const debug = Debug('qdone:consumer')

// Global flag for shutdown request
let shutdownRequested = false
const shutdownCallbacks = []


//
// Latency
//

let latencyTimeout
let latencyMeasurements = []
let reportLatencyTimeout

function reportLatency () {
  clearTimeout(reportLatencyTimeout)
  reportLatencyTimeout = setTimeout(() => {
    //console.log(latencyMeasurements)
    const meanLatency = latencyMeasurements.length ? latencyMeasurements.reduce((a, b) => a + b, 0) / latencyMeasurements.length : 0
    console.log({ meanLatency })
    reportLatency()
  }, 5000)
}

function measureLoopLatency (callCount) {
  clearTimeout(latencyTimeout)
  const start = new Date()
  latencyTimeout = setTimeout(() => {
    const latency = new Date() - start
    latencyMeasurements.push(latency)
    if (latencyMeasurements.length > 1000) {
      latencyMeasurements.shift()
    }
    // console.log(`Loop ${callCount} took\t${latency} ms`)
    if (!reportLatencyTimeout) reportLatency()
    measureLoopLatency(callCount + 1)
  })
}
function shutdownLatency () {
  console.log(latencyMeasurements)
  clearTimeout(latencyTimeout)
  clearTimeout(reportLatencyTimeout)
}

//
// Latency
//

export function requestShutdown () {
  debug('requestShutdown')
  shutdownLatency()
  shutdownRequested = true
  for (const callback of shutdownCallbacks) {
    debug('callback', callback)
    callback()
    // try { callback() } catch (e) { }
  }
  debug('requestShutdown done')
}

export async function processMessage (message, callback, qname, qrl, opt) {
  debug('processMessage', message, qname, qrl)
  const payload = opt.json ? JSON.parse(message.Body) : message.Body
  if (opt.verbose) {
    console.error(chalk.blue('  Processing payload:'), payload)
  } else if (!opt.disableLog) {
    console.log(JSON.stringify({
      event: 'MESSAGE_PROCESSING_START',
      timestamp: new Date(),
      messageId: message.MessageId,
      payload
    }))
  }

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
        ReceiptHandle: message.ReceiptHandle,
        VisibilityTimeout: visibilityTimeout
      }))
      debug('ChangeMessageVisibility returned', result)
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
      debug('ChangeMessageVisibility threw', err)
      // Rejection means we're ouuta time, whatever, let the job die
      if (opt.verbose) {
        console.error(chalk.red('  failed to extend job: ') + err)
      } else if (!opt.disableLog) {
        // Production error logging
        console.log(JSON.stringify({
          event: 'MESSAGE_PROCESSING_FAILED',
          reason: 'ran longer than --kill-after',
          timestamp: new Date(),
          messageId: message.MessageId,
          payload,
          errorMessage: err.toString().split('\n').slice(1).join('\n').trim() || undefined,
          err
        }))
      }
    }
  }

  // Extend when we get 50% of the way to timeout
  timeoutExtender = setTimeout(extendTimeout, visibilityTimeout * 1000 * 0.5)
  debug('timeout', visibilityTimeout * 1000 * 0.5)

  try {
    // Process message
    const queue = qname.slice(opt.prefix.length)
    const result = await callback(queue, payload)
    debug('processMessage callback finished', { payload, result })
    clearTimeout(timeoutExtender)
    if (opt.verbose) {
      console.error(chalk.green('  SUCCESS'))
      console.error(chalk.blue('  cleaning up (removing message) ...'))
    }
    await getSQSClient().send(new DeleteMessageCommand({
      QueueUrl: qrl,
      ReceiptHandle: message.ReceiptHandle
    }))
    if (opt.verbose) {
      console.error(chalk.blue('  done'))
      console.error()
    } else if (!opt.disableLog) {
      console.log(JSON.stringify({
        event: 'MESSAGE_PROCESSING_COMPLETE',
        timestamp: new Date(),
        messageId: message.MessageId,
        payload
      }))
    }
    return { noJobs: 0, jobsSucceeded: 1, jobsFailed: 0 }
  } catch (err) {
    debug('exec.catch')
    clearTimeout(timeoutExtender)

    // If the callback does not want to process this message, return to queue
    if (err instanceof DoNotProcess) {
      if (opt.verbose) {
        console.error(chalk.blue('  callback ') + chalk.yellow('REFUSED'))
        console.error(chalk.blue('  cleaning up (removing message) ...'))
      }
      const result = await getSQSClient().send(new ChangeMessageVisibilityCommand({
        QueueUrl: qrl,
        ReceiptHandle: message.ReceiptHandle,
        VisibilityTimeout: 0
      }))
      debug('ChangeMessageVisibility returned', result)
      return { noJobs: 1, jobsSucceeded: 0, jobsFailed: 0 }
    }

    // Fail path for job execution
    if (opt.verbose) {
      console.error(chalk.red('  FAILED'))
      console.error(chalk.blue('  error : ') + err)
    } else if (!opt.disableLog) {
      // Production error logging
      console.log(JSON.stringify({
        event: 'MESSAGE_PROCESSING_FAILED',
        reason: 'exception thrown',
        timestamp: new Date(),
        messageId: message.MessageId,
        payload,
        errorMessage: err.toString().split('\n').slice(1).join('\n').trim() || undefined,
        err
      }))
    }
    return { noJobs: 0, jobsSucceeded: 0, jobsFailed: 1 }
  }
}

//
// Pull work off of a single queue
//
export async function pollSingleQueue (qname, qrl, callback, opt) {
  debug('pollSingleQueue', { qname, qrl, callback, opt })
  const params = {
    AttributeNames: ['All'],
    MaxNumberOfMessages: 1,
    MessageAttributeNames: ['All'],
    QueueUrl: qrl,
    VisibilityTimeout: 30,
    WaitTimeSeconds: opt.waitTime
  }
  const response = await getSQSClient().send(new ReceiveMessageCommand(params))
  debug('ReceiveMessage response', response)
  if (shutdownRequested) return { noJobs: 0, jobsSucceeded: 0, jobsFailed: 0 }
  if (response.Messages) {
    const message = response.Messages[0]
    if (opt.verbose) console.error(chalk.blue('  Found message ' + message.MessageId))
    return processMessage(message, callback, qname, qrl, opt)
  } else {
    return { noJobs: 1, jobsSucceeded: 0, jobsFailed: 0 }
  }
}

//
// Resolve a set of queues
//
export async function resolveQueues (queues, opt) {
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

  return selectedPairs
}

//
// Consumer
//
export async function processMessages (queues, callback, options) {
  measureLoopLatency()
  const opt = getOptionsWithDefaults(options)
  debug('processMessages', { queues, callback, options, opt })

  let loopCounter = 0
  const stats = { noJobs: 0, jobsSucceeded: 0, jobsFailed: 0 }
  const maxActiveLoops = 10
  const activeLoops = new Map()
  const completedLoops = new Map()

  // This delay function keeps a timeout reference around so it can be
  // cancelled at shutdown
  let delayTimeout
  const delay = (ms) => new Promise(resolve => {
    delayTimeout = setTimeout(resolve, ms)
  })

  // Callback to help facilitate better UX at shutdown
  function shutdownCallback () {
    if (opt.verbose) {
      debug({ activeLoops, completedLoops })
      if (activeLoops.length) {
        console.error(chalk.blue('Waiting for work to finish on the following queues: '))
        for (const [id, { qname, qrl, promise }] of activeLoops) {
          console.error('    ' + qname + `job ${id}`)
        }
      }
      // clearTimeout(delayTimeout)
    }
  }
  shutdownCallbacks.push(shutdownCallback)

  // Listen to a queue until it is out of messages
  async function listenLoop (qname, qrl, loopId) {
    try {
      if (shutdownRequested) return
      if (opt.verbose) {
        console.error(
          chalk.blue('Looking for work on ') +
          qname.slice(opt.prefix.length) +
          chalk.blue(' (' + qrl + ')')
        )
      }
      // Aggregate the results
      const { noJobs, jobsSucceeded, jobsFailed } = await pollSingleQueue(qname, qrl, callback, opt)
      debug('pollSingleQueue return')
      stats.noJobs += noJobs
      stats.jobsFailed += jobsFailed
      stats.jobsSucceeded += jobsSucceeded
      debug({ stats, noJobs })

      // No work? Shutdown requested? Return to outer loop
      debug({ noJobs, shutdownRequested })
      if (noJobs || shutdownRequested) return

      // Otherwise keep going
      return listenLoop(qname, qrl)
    } catch (err) {
      // TODO: Sentry
      console.error(chalk.red('  ERROR in listenLoop'))
      console.error(chalk.blue('  error : ') + err)
    } finally {
      completedLoops.set(loopId, activeLoops.get(loopId))
    }
  }

  // Resolve loop
  while (!shutdownRequested) { // eslint-disable-line
    const start = new Date()
    const selectedPairs = await resolveQueues(queues, opt)
    debug({ selectedPairs })
    if (shutdownRequested) break

    // But only if we have queues to listen on
    if (selectedPairs.length) {
      // Randomize order
      selectedPairs.sort(() => 0.5 - Math.random())

      if (opt.verbose) {
        console.error(chalk.blue('Listening to queues (in this order):'))
        console.error(selectedPairs.map(({ qname, qrl }) =>
          '  ' + qname.slice(opt.prefix.length) + chalk.blue(' - ' + qrl)
        ).join('\n'))
        console.error()
      }

      // Launch listen loop for each queue
      for (const { qname, qrl } of selectedPairs) {
        // Bail if we already have too many
        if (activeLoops.size >= maxActiveLoops) {
          if (opt.verbose) console.error(chalk.yellow('Hit active worker limit of ') + maxActiveLoops)
          break
        }
        const loopId = loopCounter++
        activeLoops.set(loopId, { qname, qrl, promise: listenLoop(qname, qrl, loopId) })
      }
    }

    // Wait until the next time we need to resolve
    if (!shutdownRequested) {
      const msSoFar = Math.max(0, new Date() - start)
      const msUntilNextResolve = Math.max(0, /*opt.waitTime **/ 1000 - msSoFar)
      debug({ msSoFar, msUntilNextResolve })
      if (msUntilNextResolve) {
        if (opt.verbose) console.error(chalk.blue('Will resolve queues again in ' + Math.round(msUntilNextResolve / 1000) + ' seconds'))
        await delay(msUntilNextResolve)
      }
    }

    // Cleanup completed loops
    for (const [id, { qname, qrl, promise }] of completedLoops) {
      await promise // make sure the promise resolves
      debug('Cleaning up', { id, qname, qrl, promise })
      activeLoops.delete(id)
    }
  }
  debug('out here', { activeLoops })

  // Wait on all work to finish
  // shutdownCallback()
  for (const [id, { qname, qrl, promise }] of activeLoops) {
    debug('Waiting on active loop', id)
    await promise // make sure the promise resolves
  }
  debug('after all')
}

debug('loaded')
