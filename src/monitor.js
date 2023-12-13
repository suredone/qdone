/**
 * Multi-queue monitoring functionaliry
 */

import { getMatchingQueues, getQueueAttributes } from './sqs.js'
import Debug from 'debug'
const debug = Debug('sd:utils:qmonitor:index')

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
 * Aggregates inmportant attributes across queues and reports a summary.
 * Attributes:
 *  - ApproximateNumberOfMessages: Sum
 *  - ApproximateNumberOfMessagesDelayed: Sum
 *  - ApproximateNumberOfMessagesNotVisible: Sum
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
  // debug({ total })
  // convert set to array
  total.contributingQueueNames = [...total.contributingQueueNames.values()]
  total.queueName = queueName
  return total
}

debug('loaded')
