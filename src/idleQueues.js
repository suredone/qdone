/**
 * Implementation of checks and caching of checks to determine if queues are idle.
 */
import chalk from 'chalk'
import { getSQSClient } from './sqs.js'
import { getCloudWatchClient } from './cloudWatch.js'
import { getOptionsWithDefaults } from './defaults.js'
import { GetQueueAttributesCommand, DeleteQueueCommand, QueueDoesNotExist } from '@aws-sdk/client-sqs'
import { GetMetricStatisticsCommand } from '@aws-sdk/client-cloudwatch'
import { normalizeFailQueueName, normalizeDLQName, getQnameUrlPairs, fifoSuffix } from './qrlCache.js'
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
  debug('_cheapIdleCheck', qname, qrl)
  try {
    const client = getSQSClient()
    const cmd = new GetQueueAttributesCommand({ AttributeNames: attributeNames, QueueUrl: qrl })
    const data = await client.send(cmd)
    debug('data', data)
    const result = data.Attributes
    result.queue = qname.slice(opt.prefix.length)
    // We are idle if all the messages attributes are zero
    result.idle = attributeNames.filter(k => result[k] === '0').length === attributeNames.length
    result.exists = true
    debug({ result, SQS: 1 })
    return { result, SQS: 1 }
  } catch (e) {
    debug({ _cheapIdleCheck: e })
    if (e instanceof QueueDoesNotExist) {
      return { result: { idle: undefined, exists: false }, SQS: 1 }
    } else {
      throw e
    }
  }
}

/**
 * Gets queue attributes from the SQS api and assesses whether queue is idle
 * at this immediate moment.
 */
export async function cheapIdleCheck (qname, qrl, opt) {
  debug('cheapIdleCheck', qname, qrl)
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

  // Short circuit further calls if cheap result is conclusive
  if (cheapResult.idle === false || cheapResult.exists === false) {
    return {
      queue: qname.slice(opt.prefix.length),
      cheap: { SQS, result: cheapResult },
      idle: cheapResult.idle,
      exists: cheapResult.exists,
      apiCalls: { SQS, CloudWatch: 0 }
    }
  }

  // If we get here, there's nothing in the queue at the moment,
  // so we have to check metrics one at a time
  const apiCalls = { SQS, CloudWatch: 0 }
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
      idle,
      exists: true
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
 * Processes a queue and its fail and delete queue, treating them as a unit.
 */
export async function processQueueSet (qname, qrl, opt) {
  const isFifo = qname.endsWith('.fifo')
  const normalizeOptions = Object.assign({}, opt, { fifo: isFifo })

  // Generate DLQ name/url
  const dqname = normalizeDLQName(qname, normalizeOptions)
  const dqrl = normalizeDLQName(dqname, normalizeOptions)

  // Generate fail queue name/url
  const fqname = normalizeFailQueueName(qname, normalizeOptions)
  const fqrl = normalizeFailQueueName(fqname, normalizeOptions)

  debug({ qname, qrl, dqname, dqrl, fqname, fqrl })

  // Idle check
  const qresult = await checkIdle(qname, qrl, opt)
  const fqresult = await checkIdle(fqname, fqrl, opt)
  const dqresult = await checkIdle(dqname, dqrl, opt)
  debug({ qresult, fqresult, dqresult })

  // Start building return value
  const result = Object.assign(
    {
      queue: qname,
      idle: (
        qresult.idle &&
        (!fqresult.exists || fqresult.idle) &&
        (!fqresult.exists || dqresult.idle)
      ),
      apiCalls: {
        SQS: qresult.apiCalls.SQS + fqresult.apiCalls.SQS + dqresult.apiCalls.SQS,
        CloudWatch: qresult.apiCalls.CloudWatch + fqresult.apiCalls.CloudWatch + dqresult.apiCalls.CloudWatch
      }
    }
  )

  // Queue is idle
  if (qresult.idle && opt.verbose) console.error(chalk.blue('Queue ') + qname.slice(opt.prefix.length) + chalk.blue(' has been ') + 'idle' + chalk.blue(' for the last ') + opt.idleFor + chalk.blue(' minutes.'))
  if (fqresult.idle && opt.verbose) console.error(chalk.blue('Queue ') + fqname.slice(opt.prefix.length) + chalk.blue(' has been ') + 'idle' + chalk.blue(' for the last ') + opt.idleFor + chalk.blue(' minutes.'))
  if (dqresult.idle && opt.verbose) console.error(chalk.blue('Queue ') + dqname.slice(opt.prefix.length) + chalk.blue(' has been ') + 'idle' + chalk.blue(' for the last ') + opt.idleFor + chalk.blue(' minutes.'))

  // Delete if all are idle
  const canDelete = (
    (qresult.idle || qresult.exists === false) &&
    (fqresult.idle || fqresult.exists === false) &&
    (dqresult.idle || dqresult.exists === false)
  )
  debug({ canDelete })

  if (opt.delete && canDelete) {
    // Normal
    const qdresult = await (async () => {
      debug({ qresult })
      try {
        if (qresult.idle) return deleteQueue(qname, qrl, opt)
      } catch (e) {
        if (!(e instanceof QueueDoesNotExist)) throw e
      }
    })()
    if (qdresult) { result.apiCalls.SQS += qdresult.apiCalls.SQS }
    debug({ qdresult })

    // Fail
    const fqdresult = await (async () => {
      debug({ fqresult })
      try {
        if (fqresult.idle) return deleteQueue(fqname, fqrl, opt)
      } catch (e) {
        if (!(e instanceof QueueDoesNotExist)) throw e
      }
    })()
    if (fqdresult) { result.apiCalls.SQS += fqdresult.apiCalls.SQS }
    debug({ fqdresult })

    // Dead
    const dqdresult = await (async () => {
      debug({ dqresult })
      try {
        if (dqresult.idle) return deleteQueue(dqname, dqrl, opt)
      } catch (e) {
        if (!(e instanceof QueueDoesNotExist)) throw e
      }
    })()
    if (dqdresult) { result.apiCalls.SQS += dqdresult.apiCalls.SQS }
    debug({ dqdresult })
  }

  return result
}

//
// Strips failed and dlq suffix from a queue name or URL
//
export function stripSuffixes (queueName, opt) {
  const suffixFinder = new RegExp(`(${opt.dlqSuffix}|${opt.failSuffix}){1}(|${fifoSuffix})$`)
  return queueName.replace(suffixFinder, '$2')
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

  // Filter out failed and dead queues, but if we have an orphaned fail or
  // dead queue, keep the original parent queue name so that orphans can be
  // deleted.
  const queueNames = new Set()
  const filteredEntries = entries.filter(entry => {
    const stripped = stripSuffixes(entry.qname, opt)
    if (queueNames.has(stripped)) return false
    queueNames.add(stripped)
    entry.qname = stripped
    entry.qrl = stripSuffixes(entry.qrl, opt)
    return true
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
    return Promise.all(filteredEntries.map(e => processQueueSet(e.qname, e.qrl, opt)))
  }

  // Otherwise, let caller know
  return 'noQueues'
}

debug('loaded')
