import { createHash } from 'crypto'
import { v1 as uuidV1 } from 'uuid'
import { getCacheClient } from './cache.js'
import Debug from 'debug'
const debug = Debug('qdone:dedup')

/**
 * Returns a MessageDeduplicationId key appropriate for using with Amazon SQS
 * for the given message. The passed dedupContent will be returned untouched
 * if it meets all the requirements for SQS's MessageDeduplicationId,
 * otherwise disallowed characters will be replaced by `_` and content longer
 * than 128 characters will be truncated and a hash of the content appended.
 * @param {String} dedupContent - Content used to construct the deduplication id.
 * @param {Object} opt - Opt object from getOptionsWithDefaults()
 * @returns {String} the cache key
 */
export function getDeduplicationId (dedupContent, opt) {
  debug({ getDeduplicationId: { dedupContent } })
  // Don't transmit long keys to redis
  dedupContent = dedupContent.trim().replace(/[^a-zA-Z0-9!"#$%&'()*+,-./:;<=>?@[\\\]^_`{|}~]/g, '_')
  const max = 128
  const sep = '...sha1:'
  if (dedupContent.length > max) {
    dedupContent = dedupContent.slice(0, max - sep.length - 40) + '...sha1:' + createHash('sha1').update(dedupContent).digest('hex')
  }
  return dedupContent
}

/**
 * Returns the cache key given a deduplication id.
 * @param {String} dedupId - a deduplication id returned from getDeduplicationId
 * @param opt - Opt object from getOptionsWithDefaults()
 * @returns the cache key
 */
export function getCacheKey (dedupId, opt) {
  const cacheKey = opt.cachePrefix + 'dedup:' + dedupId
  debug({ getCacheKey: { cacheKey } })
  return cacheKey
}

/**
 * Modifies a message (parameters to SendMessageCommand) to add the parameters
 * for whatever deduplication options the caller has set.
 * @param {String} message - parameters to SendMessageCommand
 * @param {Object} opt - Opt object from getOptionsWithDefaults()
 * @returns {Object} the modified parameters/message object
 */
export function addDedupParamsToMessage (message, opt) {
  // Either of these means we need to calculate an id
  if (opt.fifo || opt.externalDedup) {
    const uuidFunction = opt.uuidFunction || uuidV1
    if (opt.deduplicationId) message.MessageDeduplicationId = opt.deduplicationId
    if (opt.dedupIdPerMessage) message.MessageDeduplicationId = uuidFunction()

    // Fallback to using the message body
    if (!message.MessageDeduplicationId) {
      message.MessageDeduplicationId = getDeduplicationId(message.MessageBody, opt)
    }

    // Track our own dedup id so we can look it up upon ReceiveMessage
    if (opt.externalDedup) {
      message.MessageAttributes = {
        QdoneDeduplicationId: {
          StringValue: message.MessageDeduplicationId,
          DataType: 'String'
        }
      }
      // If we are using our own dedup, then we must disable the SQS dedup by
      // providing a different unique ID. Otherwise SQS will interact with us.
      if (opt.fifo) message.MessageDeduplicationId = uuidFunction()
    }

    // Non fifo can't have this parameter
    if (!opt.fifo) delete message.MessageDeduplicationId
  }
  return message
}

/**
 * Updates statistics in redis, of which there are two:
 * 1. duplicateSet - a set who's members are cache keys and scores are the number of duplicate
 *                   runs prevented by dedup.
 * 2. expirationSet - a set who's members are cache keys and scores are when the cache key expires
 * @param {String} cacheKey
 * @param {Number} duplicates - the number of duplicates, must be at least 1 to gather stats
 * @param {Number} expireAt - timestamp for when this key's dedupPeriod expires
 * @param {Object} opt - Opt object from getOptionsWithDefaults()
 * @param {Object} pipeline - (Optional) redis pipeline you will exec() yourself
 */
export async function updateStats (cacheKey, duplicates, expireAt, opt, pipeline) {
  if (duplicates >= 1) {
    const duplicateSet = opt.cachePrefix + 'dedup-stats:duplicateSet'
    const expirationSet = opt.cachePrefix + 'dedup-stats:expirationSet'
    const hadPipeline = !!pipeline
    if (!hadPipeline) pipeline = getCacheClient(opt).multi()
    pipeline.zadd(duplicateSet, 'GT', duplicates, cacheKey)
    pipeline.zadd(expirationSet, 'GT', expireAt, cacheKey)
    if (!hadPipeline) await pipeline.exec()
  }
}

/**
 * Removes expired items from stats.
 */
export async function statMaintenance (opt) {
  const duplicateSet = opt.cachePrefix + 'dedup-stats:duplicateSet'
  const expirationSet = opt.cachePrefix + 'dedup-stats:expirationSet'
  const client = getCacheClient(opt)
  const now = new Date().getTime()

  // Grab a batch of expired keys
  debug({ statMaintenance: { aboutToGo: true, expirationSet }})
  const expiredStats = await client.zrange(expirationSet, '-inf', now, 'BYSCORE')
  debug({ statMaintenance: { expiredStats }})

  // And remove them from indexes, main storage
  if (expiredStats.length) {
    const result = await client.multi()
      .zrem(expirationSet, expiredStats)
      .zrem(duplicateSet, expiredStats)
      .exec()
    debug({ statMaintenance: { result }})
  }
}

/**
 * Determines whether we should enqueue this message or whether it is a duplicate.
 * Returns true if enqueuing the message would not result in a duplicate.
 * @param {Object} message - Parameters to SendMessageCommand
 * @param {Object} opt - Opt object from getOptionsWithDefaults()
 * @returns {Boolean} true if the message can be enqueued without duplicate, else false
 */
export async function dedupShouldEnqueue (message, opt) {
  const client = getCacheClient(opt)
  const dedupId = message?.MessageAttributes?.QdoneDeduplicationId?.StringValue
  const cacheKey = getCacheKey(dedupId, opt)
  const expireAt = new Date().getTime() + opt.dedupPeriod
  const copies = await client.incr(cacheKey)
  debug({ action: 'shouldEnqueue', cacheKey, copies })
  if (copies === 1) {
    await client.expireat(cacheKey, expireAt)
    return true
  }
  if (opt.dedupStats) {
    const duplicates = copies - 1
    await updateStats(cacheKey, duplicates, expireAt, opt)
  }
  return false
}

/**
 * Determines which messages we should enqueue, returning only those that
 * would not be duplicates.
 * @param {Array[Object]} messages - Entries array for the SendMessageBatchCommand
 * @param {Object} opt - Opt object from getOptionsWithDefaults()
 * @returns {Array[Object]} an array of messages that can be safely enqueued. Could be empty.
 */
export async function dedupShouldEnqueueMulti (messages, opt) {
  debug({ dedupShouldEnqueueMulti: { messages, opt }})
  const expireAt = new Date().getTime() + opt.dedupPeriod
  // Increment all
  const incrPipeline = getCacheClient(opt).pipeline()
  for (const message of messages) {
    const dedupId = message?.MessageAttributes?.QdoneDeduplicationId?.StringValue
    const cacheKey = getCacheKey(dedupId, opt)
    incrPipeline.incr(cacheKey)
  }
  const responses = await incrPipeline.exec()
  debug({ dedupShouldEnqueueMulti: { messages, responses } })

  // Figure out dedup period
  const minDedupPeriod = 6 * 60
  const dedupPeriod = Math.min(opt.dedupPeriod, minDedupPeriod)

  // Interpret responses and expire keys for races we won
  const expirePipeline = getCacheClient(opt).pipeline()
  const statsPipeline = opt.dedupStats ? getCacheClient(opt).pipeline() : undefined
  const messagesToEnqueue = []
  for (let i = 0; i < messages.length; i++) {
    const message = messages[i]
    const [, copies] = responses[i]
    const dedupId = message?.MessageAttributes?.QdoneDeduplicationId?.StringValue
    const cacheKey = getCacheKey(dedupId, opt)
    if (copies === 1) {
      messagesToEnqueue.push(message)
      expirePipeline.expireat(cacheKey, expireAt)
    } else if (opt.dedupStats) {
      const duplicates = copies - 1
      updateStats(cacheKey, duplicates, expireAt, opt, statsPipeline)
    }
  }
  await expirePipeline.exec()
  if (opt.dedupStats) await statsPipeline.exec()
  return messagesToEnqueue
}

/**
 * Marks a message as processed so that subsequent calls to dedupShouldEnqueue
 * and dedupShouldEnqueueMulti will allow a message to be enqueued again
 * without waiting for dedupPeriod to expire.
 * @param {Object} message - Return value from RecieveMessageCommand
 * @param {Object} opt - Opt object from getOptionsWithDefaults()
 * @returns {Number} 1 if a cache key was deleted, otherwise 0
 */
export async function dedupSuccessfullyProcessed (message, opt) {
  const client = getCacheClient(opt)
  const dedupId = message?.MessageAttributes?.QdoneDeduplicationId?.StringValue
  if (dedupId) {
    const cacheKey = getCacheKey(dedupId, opt)
    const count = await client.del(cacheKey)
    // Probabalistic stat maintenance
    if (opt.dedupStats) {
      const chance = 1 / 100.0
      if (Math.random() < chance) await statMaintenance(opt)
    }
    return count
  }
  return 0
}

/**
 * Marks an array of messages as processed so that subsequent calls to
 * dedupShouldEnqueue and dedupShouldEnqueueMulti will allow a message to be
 * enqueued again without waiting for dedupPeriod to expire.
 * @param {Array[Object]} messages - Return values from RecieveMessageCommand
 * @param {Object} opt - Opt object from getOptionsWithDefaults()
 * @returns {Number} number of deleted keys
 */
export async function dedupSuccessfullyProcessedMulti (messages, opt) {
  debug({ messages, dedupSuccessfullyProcessedMulti: { messages, opt }})
  const cacheKeys = []
  for (const message of messages) {
    const dedupId = message?.MessageAttributes?.QdoneDeduplicationId?.StringValue
    if (dedupId) {
      const cacheKey = getCacheKey(dedupId, opt)
      cacheKeys.push(cacheKey)
    }
  }
  debug({ dedupSuccessfullyProcessedMulti: { cacheKeys }})
  if (cacheKeys.length) {
    const numDeleted = await getCacheClient(opt).del(cacheKeys)
    // const numDeleted = results.map(([, val]) => val).reduce((a, b) => a + b, 0)
    debug({ dedupSuccessfullyProcessedMulti: { cacheKeys, numDeleted } })

    // Probabalistic stat maintenance
    if (opt.dedupStats) {
      const chance = numDeleted / 100.0
      if (Math.random() < chance) await statMaintenance(opt)
    }
    return numDeleted
  }
  return 0
}
