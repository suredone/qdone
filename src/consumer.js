/**
 * Consumer implementation.
 */

import { freemem, totalmem } from 'os'
import { ReceiveMessageCommand, QueueDoesNotExist } from '@aws-sdk/client-sqs'
import chalk from 'chalk'
import Debug from 'debug'

import { SystemMonitor } from './scheduler/systemMonitor.js'
import { QueueManager } from './scheduler/queueManager.js'
import { JobExecutor } from './scheduler/jobExecutor.js'
import { getOptionsWithDefaults } from './defaults.js'
import { getSQSClient } from './sqs.js'

const debug = Debug('qdone:consumer')

// Global flag for shutdown request
let shutdownRequested = false
const shutdownCallbacks = []

export async function requestShutdown () {
  debug('requestShutdown')
  shutdownRequested = true
  for (const callback of shutdownCallbacks) {
    debug('callback', callback)
    await callback()
    // try { callback() } catch (e) { }
  }
  debug('requestShutdown done')
}

export async function getMessages (qrl, opt, maxMessages) {
  const params = {
    AttributeNames: ['All'],
    MaxNumberOfMessages: maxMessages,
    MessageAttributeNames: ['All'],
    QueueUrl: qrl,
    VisibilityTimeout: 60,
    WaitTimeSeconds: opt.waitTime
  }
  const response = await getSQSClient().send(new ReceiveMessageCommand(params))
  // debug('ReceiveMessage response', response)
  return response.Messages || []
}

//
// Consumer
//
export async function processMessages (queues, callback, options) {
  debug({ options })
  const opt = getOptionsWithDefaults(options)
  debug('processMessages', { queues, callback, options, opt, argv: process.argv })

  let lastLatency = 10
  const systemMonitor = new SystemMonitor(latency => {
    const percentDifference = 100 * Math.abs(lastLatency - latency) / lastLatency
    if (percentDifference > 10 && opt.verbose) {
      console.error(chalk.blue('Latency:', Math.round(latency), 'ms'))
    }
    lastLatency = latency
  })
  const jobExecutor = new JobExecutor(opt)
  const queueManager = new QueueManager(opt, queues, 60)
  // debug({ systemMonitor, jobExecutor, queueManager })

  // This delay function keeps a timeout reference around so it can be
  // cancelled at shutdown
  let delayTimeout
  const delay = (ms) => new Promise(resolve => {
    delayTimeout = setTimeout(resolve, ms)
  })

  shutdownCallbacks.push(async () => {
    clearTimeout(delayTimeout)
    await queueManager.shutdown()
    debug({ queueManager: 'done' })
    await jobExecutor.shutdown()
    debug({ jobExecutor: 'done' })
    await systemMonitor.shutdown()
    debug({ systemMonitor: 'done' })
  })

  // Keep track of how many messages could be returned from each queue
  const activeQrls = new Set()
  let maxReturnCount = 0
  const listen = async (qname, qrl, maxMessages) => {
    activeQrls.add(qrl)
    maxReturnCount += maxMessages
    try {
      const messages = await getMessages(qrl, opt, maxMessages)
      if (messages.length) {
        for (const message of messages) {
          jobExecutor.executeJob(message, callback, qname, qrl)
        }
        queueManager.updateIcehouse(qrl, false)
      } else {
        // If we didn't get any, update the icehouse so we can back off
        queueManager.updateIcehouse(qrl, true)
      }

      // Max job accounting
      maxReturnCount -= maxMessages
      activeQrls.delete(qrl)
    } catch (e) {
      // If the queue has been cleaned up, we should back off anyway
      if (e instanceof QueueDoesNotExist) {
        queueManager.updateIcehouse(qrl, true)
      } else {
        throw e
      }
    }
  }

  while (!shutdownRequested) { // eslint-disable-line
    // Figure out how we are running
    const allowedJobs = Math.max(0, opt.maxConcurrentJobs - jobExecutor.activeJobCount() - maxReturnCount)
    const maxLatency = 100
    const latency = systemMonitor.getLatency() || 10
    const latencyFactor = 1 - Math.abs(Math.min(latency / maxLatency, 1)) // 0 if latency is at max, 1 if latency 0
    const freememFactor = Math.min(1, Math.max(0, freemem() - totalmem() / 2) / totalmem())
    const targetJobs = Math.round(allowedJobs * latencyFactor * freememFactor)
    let jobsLeft = targetJobs
    debug({ jobCount: jobExecutor.activeJobCount(), maxReturnCount, allowedJobs, maxLatency, latency, latencyFactor, freememFactor, targetJobs, activeQrls })
    for (const { qname, qrl } of queueManager.getPairs()) {
      debug({ evaluating: { qname, qrl, jobsLeft, activeQrlsHasQrl: activeQrls.has(qrl) } })
      if (jobsLeft <= 0 || activeQrls.has(qrl)) continue
      const maxMessages = Math.min(10, jobsLeft)
      listen(qname, qrl, maxMessages)
      jobsLeft -= maxMessages
      if (opt.verbose) {
        console.error(chalk.blue('Listening on: '), qname)
      }
      debug({ listenedTo: { qname, maxMessages, jobsLeft } })
    }
    await delay(1000)
  }
  debug('after all')
}
