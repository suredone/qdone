/**
 * Consumer implementation.
 */

import { freemem, totalmem, cpus } from 'os'
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
    VisibilityTimeout: 120,
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
  const queueManager = new QueueManager(opt, queues, 10)
  const cores = cpus().length
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
  const activeQrls = new Map()
  const listeningQrls = new Set()
  let maxReturnCount = 0
  const listen = async (qname, qrl, maxMessages) => {
    if (opt.verbose) {
      console.error(chalk.blue('Listening on: '), qname)
    }
    // Reserve concurrency and mark the queue as in-flight up front, then release
    // both in the finally so no receive-error path (including a hung socket) can
    // leak them. A leaked reservation eventually pins allowedJobs to 0, and a
    // leaked listeningQrls entry makes the queue permanently unpollable — either
    // way the worker goes silently deaf while the process still looks healthy.
    maxReturnCount += maxMessages
    listeningQrls.add(qrl)
    try {
      const messages = await getMessages(qrl, opt, maxMessages)
      // Successful receive: release the queue immediately so the scheduler can
      // issue another receive on it while this batch executes (receive/execute
      // overlap); the finally below stays as the error-path safety net.
      listeningQrls.delete(qrl)

      if (!shutdownRequested) {
        if (messages.length) {
          activeQrls.set(qrl, (activeQrls.get(qrl) || 0) + 1)
          await jobExecutor.executeJobs(messages, callback, qname, qrl)
          const count = activeQrls.get(qrl) - 1
          if (count) activeQrls.set(qrl, count)
          else activeQrls.delete(qrl)
          queueManager.updateIcehouse(qrl, false)
        } else {
          // If we didn't get any, update the icehouse so we can back off
          queueManager.updateIcehouse(qrl, true)
        }
      }
    } catch (e) {
      // Treat any receive/execute error like an empty receive: back the queue
      // off through the icehouse and swallow it. listen() is called
      // fire-and-forget, so rethrowing surfaces as an unhandled rejection (which
      // a lenient global handler may not even crash on). QueueDoesNotExist is an
      // expected, benign result of idle-queue GC, so it stays quiet.
      queueManager.updateIcehouse(qrl, true)
      if (!(e instanceof QueueDoesNotExist) && !opt.disableLog) {
        console.log(JSON.stringify({
          event: 'RECEIVE_ERROR',
          timestamp: new Date(),
          queue: qname,
          qrl,
          errorName: e?.name,
          errorMessage: e?.message
        }))
      }
    } finally {
      // Always release the accounting, whatever happened above.
      listeningQrls.delete(qrl)
      maxReturnCount -= maxMessages
    }
  }

  if (opt.verbose) {
    function printUrls () {
      console.error({ activeQrls, listeningQrls })
      if (!shutdownRequested) setTimeout(printUrls, 2000)
    }
    printUrls()
  }

  while (!shutdownRequested) { // eslint-disable-line
    // Figure out how we are running
    const runningJobs = jobExecutor.runningJobCount()
    const allowedJobs = Math.max(0, opt.maxConcurrentJobs - maxReturnCount - runningJobs)

    // Latency
    const maxLatency = 100
    const latency = systemMonitor.getLatency() || 10
    const latencyFactor = 1 - Math.abs(Math.min(latency / maxLatency, 1)) // 0 if latency is at max, 1 if latency 0

    // Memory
    const freeMemory = freemem()
    const totalMemory = totalmem()
    const memoryThreshold = totalMemory * opt.maxMemoryPercent / 100
    const freememThreshold = totalMemory - memoryThreshold
    const remainingMemory = Math.max(0, freeMemory - freememThreshold)
    const freememFactor = Math.min(1, Math.max(0, remainingMemory / memoryThreshold))

    // Load
    const oneMinuteLoad = systemMonitor.getLoad()
    const loadPerCore = oneMinuteLoad / cores
    const loadFactor = 1 - Math.min(1, Math.max(0, loadPerCore / 3))

    const overallFactor = Math.min(latencyFactor, freememFactor, loadFactor)
    const targetJobs = Math.round(allowedJobs * overallFactor)
    let jobsLeft = targetJobs

    if (opt.verbose) {
      console.error({ maxConcurrentJobs: opt.maxConcurrentJobs, maxReturnCount, runningJobs, allowedJobs, maxLatency, latencyFactor, freememFactor, loadFactor, overallFactor, targetJobs })
    }
    for (const { qname, qrl } of queueManager.getPairs()) {
      // const qcount = jobExecutor.runningJobCountForQueue(qname)
      // console.log({ evaluating: { qname, qrl, qcount, jobsLeft, activeQrlsHasQrl: activeQrls.has(qrl) } })
      if (jobsLeft <= 0) break
      if (listeningQrls.has(qrl)) continue
      const maxMessages = Math.min(10, jobsLeft)
      // listen() is guaranteed not to throw, but keep a defensive catch so a
      // future regression can never turn this fire-and-forget into a crash.
      listen(qname, qrl, maxMessages).catch(e => debug('unexpected listen rejection', e))
      jobsLeft -= maxMessages
      // debug({ listenedTo: { qname, maxMessages, jobsLeft } })
    }
    await delay(300)
  }
  debug('after all')
}
