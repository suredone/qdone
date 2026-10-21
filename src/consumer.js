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
import { getSQSClient, shutdownSQSClient } from './sqs.js'
import { shutdownCache } from './cache.js'

const debug = Debug('qdone:consumer')

// Global flag for shutdown request
let shutdownRequested = false
const shutdownCallbacks = []
let shutdownPromise
let shutdownPhase = 'running'
let shutdownStatusProvider = () => ({ phase: shutdownPhase })
// Before processMessages() starts there is nothing to terminate; the shape matches the executor's result.
let forceTerminateActiveJobsProvider = async () => ({ attemptedJobs: 0, failedSignals: [], jobs: [] })

export function getShutdownStatus () {
  return shutdownStatusProvider()
}

export function forceTerminateActiveJobs () {
  return forceTerminateActiveJobsProvider()
}

export async function requestShutdown () {
  if (shutdownPromise) return shutdownPromise

  debug('requestShutdown')
  shutdownRequested = true
  shutdownPhase = 'draining'
  shutdownPromise = (async () => {
    const failures = []
    for (const callback of shutdownCallbacks) {
      debug('callback', callback)
      try {
        await callback()
      } catch (error) {
        failures.push(error)
      }
    }

    // Cache and SQS are last: callbacks drain every accepted listener/job and
    // perform the final message delete/dedup pass before either client closes.
    const resourceResults = await Promise.allSettled([
      shutdownCache(),
      shutdownSQSClient()
    ])
    failures.push(...resourceResults.filter(result => result.status === 'rejected').map(result => result.reason))
    debug('requestShutdown done')

    if (failures.length) {
      shutdownPhase = 'failed'
      throw new AggregateError(failures, 'qdone shutdown failed')
    }
    shutdownPhase = 'complete'
  })()

  return shutdownPromise
}

export async function getMessages (qrl, opt, maxMessages, abortSignal) {
  const params = {
    AttributeNames: ['All'],
    MaxNumberOfMessages: maxMessages,
    MessageAttributeNames: ['All'],
    QueueUrl: qrl,
    VisibilityTimeout: 120,
    WaitTimeSeconds: opt.waitTime
  }
  const response = await getSQSClient().send(new ReceiveMessageCommand(params), { abortSignal })
  // debug('ReceiveMessage response', response)
  return response.Messages || []
}

//
// Consumer
//
export async function processMessages (queues, callback, options) {
  // A signal can arrive after handlers are installed but before the consumer
  // starts. Do not construct monitors/managers against clients that shutdown
  // has already closed.
  if (shutdownRequested) return shutdownPromise

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
  forceTerminateActiveJobsProvider = () => jobExecutor.forceTerminateActiveJobs()
  const queueManager = new QueueManager(opt, queues, 10)
  const cores = cpus().length
  // debug({ systemMonitor, jobExecutor, queueManager })

  // This delay function keeps a timeout reference around so it can be
  // cancelled at shutdown
  let delayTimeout
  let resolveDelay
  const delay = (ms) => new Promise(resolve => {
    resolveDelay = resolve
    delayTimeout = setTimeout(resolve, ms)
  })

  const listenPromises = new Set()
  const receiveControllers = new Set()
  shutdownStatusProvider = () => ({
    phase: shutdownPhase,
    listeners: listenPromises.size,
    receives: receiveControllers.size,
    ...jobExecutor.shutdownStatus()
  })

  shutdownCallbacks.push(async () => {
    clearTimeout(delayTimeout)
    resolveDelay?.()
    systemMonitor.shutdown()

    // Stop discovery and new receives. An opted-in worker drains requests already
    // sent to SQS because aborting locally cannot undo an accepted delivery.
    const queueShutdown = queueManager.shutdown()
    if (!opt.drainPendingReceives) {
      for (const controller of receiveControllers) controller.abort()
    }

    const queueResult = await Promise.allSettled([queueShutdown])
    const listenerResults = await Promise.allSettled([...listenPromises])
    const listenerFailures = listenerResults.filter(result => result.status === 'rejected')
    for (const failure of listenerFailures) debug('listen failed during shutdown', failure.reason)

    // Listener promises encompass callback execution, including FIFO waits.
    // The executor then performs its final delete/dedup maintenance pass.
    await jobExecutor.shutdown()

    const failures = queueResult.filter(result => result.status === 'rejected').map(result => result.reason)
    failures.push(...listenerFailures.map(result => result.reason))
    if (failures.length) throw new AggregateError(failures, 'qdone consumer drain failed')

    debug({ queueManager: 'done', listeners: 'done', jobExecutor: 'done', systemMonitor: 'done' })
  })

  // Keep track of how many messages could be returned from each queue
  const activeQrls = new Map()
  const listeningQrls = new Set()
  let maxReturnCount = 0
  const listen = async (qname, qrl, maxMessages) => {
    if (opt.verbose) {
      console.error(chalk.blue('Listening on: '), qname)
    }
    // Receive reservations transfer to registered jobs once, including queue
    // ownership so an older batch cannot release a newer pending receive.
    maxReturnCount += maxMessages
    let reservedMessages = maxMessages
    /** @returns {void} */
    const releaseReceiveReservation = () => {
      if (!reservedMessages) return
      maxReturnCount -= reservedMessages
      reservedMessages = 0
      listeningQrls.delete(qrl)
    }
    listeningQrls.add(qrl)
    const receiveController = new AbortController()
    receiveControllers.add(receiveController)
    try {
      const messages = await getMessages(qrl, opt, maxMessages, receiveController.signal)
      receiveControllers.delete(receiveController)
      if (!shutdownRequested || opt.drainPendingReceives) {
        if (messages.length) {
          activeQrls.set(qrl, (activeQrls.get(qrl) || 0) + 1)
          const execution = jobExecutor.executeJobs(messages, callback, qname, qrl)
          // Registered waiting/running jobs now own the received capacity.
          releaseReceiveReservation()
          try {
            await execution
          } finally {
            const count = activeQrls.get(qrl) - 1
            if (count) activeQrls.set(qrl, count)
            else activeQrls.delete(qrl)
          }
          queueManager.updateIcehouse(qrl, false)
        } else {
          // If we didn't get any, update the icehouse so we can back off
          queueManager.updateIcehouse(qrl, true)
        }
      }
    } catch (e) {
      // Retain the original receive/execute-error backoff even with an empty-receive cap.
      // Back off through the icehouse and swallow it. listen() is called
      // fire-and-forget, so rethrowing surfaces as an unhandled rejection (which
      // a lenient global handler may not even crash on). QueueDoesNotExist is an
      // expected, benign result of idle-queue GC, so it stays quiet.
      queueManager.updateIcehouse(qrl, true, true)
      const expectedShutdownAbort = shutdownRequested && (e?.name === 'AbortError' || e?.name === 'TimeoutError')
      if (!(e instanceof QueueDoesNotExist) && !expectedShutdownAbort && !opt.disableLog) {
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
      receiveControllers.delete(receiveController)
      releaseReceiveReservation()
    }
  }

  const scheduleListen = (qname, qrl, maxMessages) => {
    const listenPromise = listen(qname, qrl, maxMessages)
    listenPromises.add(listenPromise)
    listenPromise.finally(() => listenPromises.delete(listenPromise)).catch(e => {
      debug('unexpected listen rejection', e)
    })
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
    const activeJobs = jobExecutor.activeJobCount()
    const allowedJobs = Math.max(0, opt.maxConcurrentJobs - maxReturnCount - activeJobs)

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
      console.error({ maxConcurrentJobs: opt.maxConcurrentJobs, maxReturnCount, activeJobs, runningJobs, allowedJobs, maxLatency, latencyFactor, freememFactor, loadFactor, overallFactor, targetJobs })
    }
    for (const { qname, qrl } of queueManager.getPairs()) {
      // const qcount = jobExecutor.runningJobCountForQueue(qname)
      // console.log({ evaluating: { qname, qrl, qcount, jobsLeft, activeQrlsHasQrl: activeQrls.has(qrl) } })
      if (jobsLeft <= 0) break
      if (listeningQrls.has(qrl)) continue
      const maxMessages = Math.min(opt.batchLimit, jobsLeft)
      scheduleListen(qname, qrl, maxMessages)
      jobsLeft -= maxMessages
      // debug({ listenedTo: { qname, maxMessages, jobsLeft } })
    }
    await delay(300)
  }
  if (shutdownPromise) await shutdownPromise
  debug('after all')
}
