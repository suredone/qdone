/**
 * Consumer implementation.
 */

import { ReceiveMessageCommand } from '@aws-sdk/client-sqs'
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

export function requestShutdown () {
  debug('requestShutdown')
  shutdownRequested = true
  for (const callback of shutdownCallbacks) {
    debug('callback', callback)
    callback()
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
    VisibilityTimeout: 30,
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
  const opt = getOptionsWithDefaults(options)
  debug('processMessages', { queues, callback, options, opt })

  const systemMonitor = new SystemMonitor(opt)
  const jobExecutor = new JobExecutor(opt)
  const queueManager = new QueueManager(opt, queues)
  const maxActiveJobs = 100
  debug({ systemMonitor, jobExecutor, queueManager })

  shutdownCallbacks.push(() => {
    systemMonitor.shutdown()
    queueManager.shutdown()
    jobExecutor.shutdown()
  })

  // This delay function keeps a timeout reference around so it can be
  // cancelled at shutdown
  let delayTimeout
  const delay = (ms) => new Promise(resolve => {
    delayTimeout = setTimeout(resolve, ms)
  })

  // Keep track of how many messages could be returned from each queue
  const activeQrls = new Set()
  let maxReturnCount = 0
  const listen = async (qname, qrl, maxMessages) => {
    activeQrls.add(qrl)
    maxReturnCount += maxMessages
    const messages = await getMessages(qrl, opt, maxMessages)
    for (const message of messages) {
      jobExecutor.executeJob(message, callback, qname, qrl)
    }
    maxReturnCount -= maxMessages
    activeQrls.delete(qrl)
  }

  while (!shutdownRequested) { // eslint-disable-line
    // Figure out how we are running
    const allowedJobs = maxActiveJobs - jobExecutor.activeJobCount() - maxReturnCount
    const maxLatency = 100
    const latency = systemMonitor.getLatency().setTimeout
    const latencyFactor = 1 - Math.abs(Math.min(latency / maxLatency, 1)) // 0 if latency is at max, 1 if latency 0
    const targetJobs = Math.round(allowedJobs * latencyFactor)
    debug({ allowedJobs, maxLatency, latency, latencyFactor, targetJobs, activeQrls })

    let jobsLeft = targetJobs
    for (const { qname, qrl } of queueManager.getPairs()) {
      if (jobsLeft <= 0 || activeQrls.has(qrl)) continue
      const maxMessages = Math.min(10, jobsLeft)
      listen(qname, qrl, maxMessages)
      jobsLeft -= maxMessages
      if (this.opt.verbose) {
        console.error(chalk.blue('Listening on: '), qname)
      }
      debug({ listenedTo: { qname, maxMessages, jobsLeft } })
    }
    await delay(1000)
  }
  debug('after all')
}
