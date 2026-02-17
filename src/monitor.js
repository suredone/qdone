/**
 * Multi-queue monitoring functionaliry
 */

import { getMatchingQueues, getQueueAttributes } from './sqs.js'
import { putAggregateData, getCloudWatchClient } from './cloudWatch.js'
import { GetMetricStatisticsCommand } from '@aws-sdk/client-cloudwatch'
import { getOptionsWithDefaults } from './defaults.js'
import { normalizeQueueName } from './qrlCache.js'
import Debug from 'debug'
const debug = Debug('qdone:monitor')

/**
 * Splits a queue name with a single wildcard into prefix and suffix regex.
 */
export async function monitor (queue, save, options) {
  if (queue.endsWith('.fifo')) options.fifo = true
  const opt = getOptionsWithDefaults(options)
  const queueName = normalizeQueueName(queue, opt)
  debug({ options, opt, queue, queueName })
  const data = await getAggregateData(queueName)
  console.log(data)
  if (save) {
    if (opt.verbose) process.stderr.write('Saving to CloudWatch...')
    await putAggregateData(data)
    if (opt.verbose) process.stderr.write('done\n')
  }
}

/**
 * Splits a queue name with a single wildcard into prefix and suffix regex.
 */
export function interpretWildcard (queueName) {
  const [prefix, suffix] = queueName.split('*')
  // Strip anything that could cause backreferences
  const safeSuffix = (suffix || '').replace(/[^a-zA-Z0-9_.]+/g, '').replace(/\./g, '\\.')
  const suffixRegex = new RegExp(`${safeSuffix}$`)
  // debug({ prefix, suffix, safeSuffix, suffixRegex })
  return { prefix, suffix, safeSuffix, suffixRegex }
}

/**
 * Gets ApproximateAgeOfOldestMessage for a single queue from CloudWatch.
 * This metric is not available via the SQS GetQueueAttributes API.
 */
export async function getQueueAge (queueName) {
  const now = new Date()
  const params = {
    StartTime: new Date(now.getTime() - 1000 * 60 * 5),
    EndTime: now,
    MetricName: 'ApproximateAgeOfOldestMessage',
    Namespace: 'AWS/SQS',
    Period: 300,
    Dimensions: [{ Name: 'QueueName', Value: queueName }],
    Statistics: ['Maximum']
  }
  const client = getCloudWatchClient()
  const cmd = new GetMetricStatisticsCommand(params)
  const data = await client.send(cmd)
  debug('getQueueAge', queueName, data)
  if (!data.Datapoints || data.Datapoints.length === 0) return 0
  return Math.max(...data.Datapoints.map(d => d.Maximum))
}

/**
 * Aggregates inmportant attributes across queues and reports a summary.
 * Attributes (from SQS GetQueueAttributes):
 *  - ApproximateNumberOfMessages: Sum
 *  - ApproximateNumberOfMessagesDelayed: Sum
 *  - ApproximateNumberOfMessagesNotVisible: Sum
 * Metrics (from CloudWatch):
 *  - ApproximateAgeOfOldestMessage: Max
 */
export async function getAggregateData (queueName) {
  const { prefix, suffixRegex } = interpretWildcard(queueName)
  const qrls = await getMatchingQueues(prefix, suffixRegex)
  // debug({ qrls })
  const data = await getQueueAttributes(qrls)
  // debug({ data })
  const total = { totalQueues: 0, contributingQueueNames: new Set() }
  for (const { queue, result } of data) {
    // debug({ row })
    total.totalQueues++
    for (const key in result.Attributes) {
      const newAtrribute = parseInt(result.Attributes[key], 10)
      if (newAtrribute > 0) {
        total.contributingQueueNames.add(queue)
        total[key] = (total[key] || 0) + newAtrribute
      }
    }
  }

  // Fetch ApproximateAgeOfOldestMessage from CloudWatch (not available via SQS API)
  const ageResults = await Promise.all(
    data.map(({ queue }) => getQueueAge(queue))
  )
  total.ApproximateAgeOfOldestMessage = Math.max(0, ...ageResults)

  // debug({ total })
  // convert set to array
  total.contributingQueueNames = [...total.contributingQueueNames.values()]
  total.queueName = queueName
  return total
}

debug('loaded')
