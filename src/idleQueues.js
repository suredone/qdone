/**
 * Implementation of checks and caching of checks to determine if queues are idle.
 */
import chalk from 'chalk'
import { getSQSClient } from './sqs.js'
import { getCloudWatchClient } from './cloudWatch.js'
import { getOptionsWithDefaults } from './defaults.js'
import { GetQueueAttributesCommand, DeleteQueueCommand, QueueDoesNotExist } from '@aws-sdk/client-sqs'
import { GetMetricStatisticsCommand } from '@aws-sdk/client-cloudwatch'
import { normalizeFailQueueName, getQnameUrlPairs, fifoSuffix } from './qrlCache.js'
import { getCache, setCache } from './cache.js'
// const AWS = require('aws-sdk')

import Debug from 'debug'
const debug = Debug('qdone:idleQueues')

// Queue attributes we check to determine idle
export const attributeNames = [
  'ApproximateNumberOfMessages',
  'ApproximateNumberOfMessagesNotVisible',
  'ApproximateNumberOfMessagesDelayed'
]

// CloudWatch metrics we check to determine idle
const metricNames = [
  'NumberOfMessagesSent',
  'NumberOfMessagesReceived',
  'NumberOfMessagesDeleted',
  'ApproximateNumberOfMessagesVisible',
  'ApproximateNumberOfMessagesNotVisible',
  'ApproximateNumberOfMessagesDelayed',
  //    'NumberOfEmptyReceives',
  'ApproximateAgeOfOldestMessage'
]

/**
 * Actual SQS call, used in conjunction with cache.
 */
export async function _cheapIdleCheck (qname, qrl, opt) {
  const client = getSQSClient()
  const cmd = new GetQueueAttributesCommand({ AttributeNames: attributeNames, QueueUrl: qrl })
  const data = await client.send(cmd)
  debug('data', data)
  const result = data.Attributes
  result.queue = qname.slice(opt.prefix.length)
  // We are idle if all the messages attributes are zero
  result.idle = attributeNames.filter(k => result[k] === '0').length === attributeNames.length
  return { result, SQS: 1 }
}

/**
 * Gets queue attributes from the SQS api and assesses whether queue is idle
 * at this immediate moment.
 */
export async function cheapIdleCheck (qname, qrl, opt) {
  // Just call the API if we don't have a cache
  if (!opt.cacheUri) return _cheapIdleCheck(qname, qrl, opt)

  // Otherwise check cache
  const key = 'cheap-idle-check:' + qrl
  const cacheResult = await getCache(key, opt)
  debug({ cacheResult })
  if (cacheResult) {
    debug({ action: 'return resolved' })
    return { result: cacheResult, SQS: 0 }
  } else {
    // Cache miss, make call
    debug({ action: 'do real check' })
    const { result, SQS } = await _cheapIdleCheck(qname, qrl, opt)
    debug({ action: 'setCache', key, result })
    const ok = await setCache(key, result, opt)
    debug({ action: 'return result of set cache', ok })
    return { result, SQS }
  }
}

/**
 * Gets a single metric from the CloudWatch api.
 */
export async function getMetric (qname, qrl, metricName, opt) {
  debug('getMetric', qname, qrl, metricName)
  const now = new Date()
  const params = {
    StartTime: new Date(now.getTime() - 1000 * 60 * opt.idleFor),
    EndTime: now,
    MetricName: metricName,
    Namespace: 'AWS/SQS',
    Period: 3600,
    Dimensions: [{ Name: 'QueueName', Value: qname }],
    Statistics: ['Sum']
    // Unit: ['']
  }
  const client = getCloudWatchClient()
  const cmd = new GetMetricStatisticsCommand(params)
  const data = await client.send(cmd)
  debug('getMetric data', data)
  return {
    [metricName]: data.Datapoints.map(d => d.Sum).reduce((a, b) => a + b, 0)
  }
}

/**
 * Checks if a single queue is idle. First queries the cheap ($0.40/1M) SQS API and
 * continues to the expensive ($10/1M) CloudWatch API, checking to make sure there
 * were no messages in the queue in the specified time period. Only if all relevant
 * metrics show no messages, is it ok to delete the queue.
 *
 * Realistically, the number of CloudWatch calls will depend on usage patterns, but
 * I see a ~70% reduction in calls by checking metrics in the given order.
 * We could randomize the order, but for my test use case, it's always cheaper
 * to check NumberOfMessagesSent first, and is the primary indicator of use.
 */
export async function checkIdle (qname, qrl, opt) {
  // Do the cheap check first to make sure there is no data in flight at the moment
  debug('checkIdle', qname, qrl)
  const { result: cheapResult, SQS } = await cheapIdleCheck(qname, qrl, opt)
  debug('cheapResult', cheapResult)

  // Short circuit further calls if cheap result shows data
  if (cheapResult.idle === false) {
    return {
      queue: qname.slice(opt.prefix.length),
      cheap: cheapResult,
      idle: false,
      apiCalls: { SQS, CloudWatch: 0 }
    }
  }

  // If we get here, there's nothing in the queue at the moment,
  // so we have to check metrics one at a time
  const apiCalls = { SQS: 1, CloudWatch: 0 }
  const results = []
  let idle = true
  for (const metricName of metricNames) {
    // Check metrics in order
    const result = await getMetric(qname, qrl, metricName, opt)
    results.push(result)
    debug('getMetric result', result)
    apiCalls.CloudWatch++

    // Recalculate idle
    idle = result[metricName] === 0
    if (!idle) break // and stop checking metrics if we find evidence of activity
  }

  // Calculate stats
  const stats = Object.assign(
    {
      queue: qname.slice(opt.prefix.length),
      cheap: cheapResult,
      apiCalls,
      idle
    },
    ...results // merge in results from CloudWatch
  )
  debug('checkIdle stats', stats)
  return stats
}

/**
 * Just deletes a queue.
 */
export async function deleteQueue (qname, qrl, opt) {
  const cmd = new DeleteQueueCommand({ QueueUrl: qrl })
  const result = await getSQSClient().send(cmd)
  debug(result)
  if (opt.verbose) console.error(chalk.blue('Deleted ') + qname.slice(opt.prefix.length))
  return {
    deleted: true,
    apiCalls: { SQS: 1, CloudWatch: 0 }
  }
}

/**
 * Processes a single queue, checking for idle, deleting if applicable.
 */
export async function processQueue (qname, qrl, opt) {
  const result = await checkIdle(qname, qrl, opt)
  debug(qname, result)

  // Queue is active
  if (!result.idle) {
    // Notify and return
    if (opt.verbose) console.error(chalk.blue('Queue ') + qname.slice(opt.prefix.length) + chalk.blue(' has been ') + 'active' + chalk.blue(' in the last ') + opt.idleFor + chalk.blue(' minutes.'))
    return result
  }

  // Queue is idle
  if (opt.verbose) console.error(chalk.blue('Queue ') + qname.slice(opt.prefix.length) + chalk.blue(' has been ') + 'idle' + chalk.blue(' for the last ') + opt.idleFor + chalk.blue(' minutes.'))
  if (opt.delete) {
    const deleteResult = await deleteQueue(qname, qrl, opt)
    const resultIncludingDelete = Object.assign(result, {
      deleted: deleteResult.deleted,
      apiCalls: {
        SQS: result.apiCalls.SQS + deleteResult.apiCalls.SQS,
        CloudWatch: result.apiCalls.CloudWatch + deleteResult.apiCalls.CloudWatch
      }
    })
    return resultIncludingDelete
  }
}

/**
 * Processes a queue and its fail queue, treating them as a unit.
 */
export async function processQueuePair (qname, qrl, opt) {
  const isFifo = qname.endsWith('.fifo')
  const normalizeOptions = Object.assign({}, opt, { fifo: isFifo })
  // Generate fail queue name/url
  const fqname = normalizeFailQueueName(qname, normalizeOptions)
  const fqrl = normalizeFailQueueName(fqname, normalizeOptions)

  // Idle check
  const result = await checkIdle(qname, qrl, opt)
  debug('result', result)

  // Queue is active
  const active = !result.idle
  if (active) {
    if (opt.verbose) console.error(chalk.blue('Queue ') + qname.slice(opt.prefix.length) + chalk.blue(' has been ') + 'active' + chalk.blue(' in the last ') + opt.idleFor + chalk.blue(' minutes.'))
    return result
  }

  // Queue is idle
  if (opt.verbose) console.error(chalk.blue('Queue ') + qname.slice(opt.prefix.length) + chalk.blue(' has been ') + 'idle' + chalk.blue(' for the last ') + opt.idleFor + chalk.blue(' minutes.'))

  // Check fail queue
  try {
    const fresult = await checkIdle(fqname, fqrl, opt)
    debug('fresult', fresult)
    const idleCheckResult = Object.assign(
      result,
      { idle: result.idle && fresult.idle, failq: fresult },
      {
        apiCalls: {
          SQS: result.apiCalls.SQS + fresult.apiCalls.SQS,
          CloudWatch: result.apiCalls.CloudWatch + fresult.apiCalls.CloudWatch
        }
      }
    )

    // Queue is active
    const factive = !fresult.idle
    if (factive) {
      if (opt.verbose) console.error(chalk.blue('Queue ') + fqname.slice(opt.prefix.length) + chalk.blue(' has been ') + 'active' + chalk.blue(' in the last ') + opt.idleFor + chalk.blue(' minutes.'))
      return idleCheckResult
    }

    // Queue is idle
    if (opt.verbose) console.error(chalk.blue('Queue ') + fqname.slice(opt.prefix.length) + chalk.blue(' has been ') + 'idle' + chalk.blue(' for the last ') + opt.idleFor + chalk.blue(' minutes.'))

    // Trigger a delete if the user wants it
    if (!opt.delete) return idleCheckResult
    const [dresult, dfresult] = await Promise.all([
      deleteQueue(qname, qrl, opt),
      deleteQueue(fqname, fqrl, opt)
    ])
    return Object.assign(idleCheckResult, {
      apiCalls: {
        // Sum the SQS calls across all four
        SQS: [result, fresult, dresult, dfresult]
          .map(r => r.apiCalls.SQS)
          .reduce((a, b) => a + b, 0),
        // Sum the CloudWatch calls across all four
        CloudWatch: [result, fresult, dresult, dfresult]
          .map(r => r.apiCalls.CloudWatch)
          .reduce((a, b) => a + b, 0)
      }
    })
  } catch (e) {
    // Handle the case where the fail queue has been deleted or was never
    // created for some reason
    if (!(e instanceof QueueDoesNotExist)) throw e

    // Fail queue doesn't exist if we get here
    if (opt.verbose) console.error(chalk.blue('Queue ') + fqname.slice(opt.prefix.length) + chalk.blue(' does not exist.'))

    // Handle delete
    if (!opt.delete) return result
    const deleteResult = await deleteQueue(qname, qrl, opt)
    const resultIncludingDelete = Object.assign(result, {
      deleted: deleteResult.deleted,
      apiCalls: {
        SQS: result.apiCalls.SQS + deleteResult.apiCalls.SQS,
        CloudWatch: result.apiCalls.CloudWatch + deleteResult.apiCalls.CloudWatch
      }
    })
    return resultIncludingDelete
  }
}

//
// Resolve queues for listening loop listen
//
export async function idleQueues (queues, options) {
  const opt = getOptionsWithDefaults(options)
  if (opt.verbose) console.error(chalk.blue('Resolving queues: ') + queues.join(' '))
  const qnames = queues.map(queue => opt.prefix + queue)
  const entries = await getQnameUrlPairs(qnames, opt)
  debug('getQnameUrlPairs.then')
  if (opt.verbose) {
    console.error(chalk.blue('  done'))
    console.error()
  }

  // Filter out any queue ending in suffix unless --include-failed is set
  const filteredEntries = entries.filter(entry => {
    const suf = opt.failSuffix
    const sufFifo = opt.failSuffix + fifoSuffix
    const isFail = entry.qname.endsWith(suf)
    const isFifoFail = entry.qname.endsWith(sufFifo)
    return opt.includeFailed ? true : (!isFail && !isFifoFail)
  })

  // But only if we have queues to remove
  if (filteredEntries.length) {
    if (opt.verbose) {
      console.error(chalk.blue('Checking queues (in this order):'))
      console.error(filteredEntries.map(e =>
        '  ' + e.qname.slice(opt.prefix.length) + chalk.blue(' - ' + e.qrl)
      ).join('\n'))
      console.error()
    }
    // Check each queue in parallel
    if (opt.unpair) return Promise.all(filteredEntries.map(e => processQueue(e.qname, e.qrl, opt)))
    return Promise.all(filteredEntries.map(e => processQueuePair(e.qname, e.qrl, opt)))
  }

  // Otherwise, let caller know
  return 'noQueues'
}

debug('loaded')
