/**
 * Implementation of checks and caching of checks to determine if queues are idle.
 */
import chalk from 'chalk'
import { getSQSClient as getSQSClient } from './sqs.js'
import { getCloudWatchClient } from './cloudWatch.js'
import { GetQueueAttributesCommand, DeleteQueueCommand } from '@aws-sdk/client-sqs'
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
export async function _cheapIdleCheck (qname, qrl, options) {
  const client = getSQSClient()
  const cmd = new GetQueueAttributesCommand({ AttributeNames: attributeNames, QueueUrl: qrl })
  const data = await client.send(cmd)
  // debug('data', data)
  const result = data.Attributes
  result.queue = qname.slice(options.prefix.length)
  // We are idle if all the messages attributes are zero
  result.idle = attributeNames.filter(k => result[k] === '0').length === attributeNames.length
  return { result, SQS: 1 }
}

/**
 * Gets queue attributes from the SQS api and assesses whether queue is idle
 * at this immediate moment.
 */
export async function cheapIdleCheck (qname, qrl, options) {
  // Just call the API if we don't have a cache
  if (!options['cache-uri']) return _cheapIdleCheck(qname, qrl, options)

  // Otherwise check cache
  const key = 'cheap-idle-check:' + qrl
  const cacheResult = await getCache(key, options)
  debug({ cacheResult })
  if (cacheResult) {
    debug({ action: 'return resolved' })
    return { result: cacheResult, SQS: 0 }
  } else {
    // Cache miss, make call
    debug({ action: 'do real check' })
    const { result, SQS } = await _cheapIdleCheck(qname, qrl, options)
    debug({ action: 'setCache', key, result })
    const ok = await setCache(key, result, options)
    debug({ action: 'return result of set cache', ok })
    return { result, SQS }
  }
}

/**
 * Gets a single metric from the CloudWatch api.
 */
export async function getMetric (qname, qrl, metricName, options) {
  debug('getMetric', qname, qrl, metricName)
  const now = new Date()
  const params = {
    StartTime: new Date(now.getTime() - 1000 * 60 * options['idle-for']),
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
export async function checkIdle (qname, qrl, options) {
  // Do the cheap check first to make sure there is no data in flight at the moment
  debug('checkIdle', qname, qrl)
  const { result: cheapResult, SQS } = await cheapIdleCheck(qname, qrl, options)
  debug('cheapResult', cheapResult)

  // Short circuit further calls if cheap result shows data
  if (cheapResult.idle === false) {
    return {
      queue: qname.slice(options.prefix.length),
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
    const result = await getMetric(qname, qrl, metricName, options)
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
      queue: qname.slice(options.prefix.length),
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
export async function deleteQueue (qname, qrl, options) {
  const cmd = new DeleteQueueCommand({ QueueUrl: qrl })
  const result = await getSQSClient().send(cmd)
  debug(result)
  if (options.verbose) console.error(chalk.blue('Deleted ') + qname.slice(options.prefix.length))
  return {
    deleted: true,
    apiCalls: { SQS: 1, CloudWatch: 0 }
  }
}

/**
 * Processes a single queue, checking for idle, deleting if applicable.
 */
export async function processQueue (qname, qrl, options) {
  const result = await checkIdle(qname, qrl, options)
  debug(qname, result)

  // Queue is active
  if (!result.idle) {
    // Notify and return
    if (options.verbose) console.error(chalk.blue('Queue ') + qname.slice(options.prefix.length) + chalk.blue(' has been ') + 'active' + chalk.blue(' in the last ') + options['idle-for'] + chalk.blue(' minutes.'))
    return result
  }

  // Queue is idle
  if (options.verbose) console.error(chalk.blue('Queue ') + qname.slice(options.prefix.length) + chalk.blue(' has been ') + 'idle' + chalk.blue(' for the last ') + options['idle-for'] + chalk.blue(' minutes.'))
  if (options.delete) {
    const deleteResult = await deleteQueue(qname, qrl, options)
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
export async function processQueuePair (qname, qrl, options) {
  const isFifo = qname.endsWith('.fifo')
  const normalizeOptions = Object.assign({}, options, { fifo: isFifo })
  // Generate fail queue name/url
  const fqname = normalizeFailQueueName(qname, normalizeOptions)
  const fqrl = normalizeFailQueueName(qrl, normalizeOptions)

  // Idle check
  const result = await checkIdle(qname, qrl, options)
  debug('result', result)

  // Queue is active
  const active = !result.idle
  if (active) {
    if (options.verbose) console.error(chalk.blue('Queue ') + qname.slice(options.prefix.length) + chalk.blue(' has been ') + 'active' + chalk.blue(' in the last ') + options['idle-for'] + chalk.blue(' minutes.'))
    return result
  }

  // Queue is idle
  if (options.verbose) console.error(chalk.blue('Queue ') + qname.slice(options.prefix.length) + chalk.blue(' has been ') + 'idle' + chalk.blue(' for the last ') + options['idle-for'] + chalk.blue(' minutes.'))

  // Check fail queue
  try {
    const fresult = await checkIdle(fqname, fqrl, options)
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
      if (options.verbose) console.error(chalk.blue('Queue ') + fqname.slice(options.prefix.length) + chalk.blue(' has been ') + 'active' + chalk.blue(' in the last ') + options['idle-for'] + chalk.blue(' minutes.'))
      return idleCheckResult
    }

    // Queue is idle
    if (options.verbose) console.error(chalk.blue('Queue ') + fqname.slice(options.prefix.length) + chalk.blue(' has been ') + 'idle' + chalk.blue(' for the last ') + options['idle-for'] + chalk.blue(' minutes.'))

    // Trigger a delete if the user wants it
    if (!options.delete) return idleCheckResult
    const [dresult, dfresult] = await Promise.all([
      deleteQueue(qname, qrl, options),
      deleteQueue(fqname, fqrl, options)
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
    if (e.code !== 'AWS.SimpleQueueService.NonExistentQueue') throw e

    // Fail queue doesn't exist if we get here
    if (options.verbose) console.error(chalk.blue('Queue ') + fqname.slice(options.prefix.length) + chalk.blue(' does not exist.'))

    // Handle delete
    if (!options.delete) return result
    const deleteResult = await deleteQueue(qname, qrl, options)
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
  if (options.verbose) console.error(chalk.blue('Resolving queues: ') + queues.join(' '))
  const qnames = queues.map(queue => options.prefix + queue)
  const entries = await getQnameUrlPairs(qnames, options)
  debug('getQnameUrlPairs.then')
  if (options.verbose) {
    console.error(chalk.blue('  done'))
    console.error()
  }

  // Filter out any queue ending in suffix unless --include-failed is set
  const filteredEntries = entries.filter(entry => {
    const suf = options['fail-suffix']
    const sufFifo = options['fail-suffix'] + fifoSuffix
    const isFail = entry.qname.endsWith(suf)
    const isFifoFail = entry.qname.endsWith(sufFifo)
    return options['include-failed'] ? true : (!isFail && !isFifoFail)
  })

  // But only if we have queues to remove
  if (filteredEntries.length) {
    if (options.verbose) {
      console.error(chalk.blue('Checking queues (in this order):'))
      console.error(filteredEntries.map(e =>
        '  ' + e.qname.slice(options.prefix.length) + chalk.blue(' - ' + e.qrl)
      ).join('\n'))
      console.error()
    }
    // Check each queue in parallel
    if (options.unpair) return Promise.all(filteredEntries.map(e => processQueue(e.qname, e.qrl, options)))
    return Promise.all(filteredEntries.map(e => processQueuePair(e.qname, e.qrl, options)))
  }

  // Otherwise, let caller know
  return 'noQueues'
}

debug('loaded')
