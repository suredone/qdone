import { createHash } from 'crypto'
import { v1 as uuidV1 } from 'uuid'
import { getCacheClient } from './cache.js'
import { claimOwner, confirmOwner, releaseOwner } from './dedupOwnership.js'
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
  const sep = 'sha1::body:'
  const hash = createHash('sha1').update(dedupContent).digest('hex')
  const truncated = dedupContent.slice(0, max - sep.length - 42)
  const id = `sha1:{${hash}}:body:${truncated}`
  return id
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
 * @param {Object} [messageOptions] - optional per message options. We only care about the key deduplicationId.
 * @returns {Object} the modified parameters/message object
 */
export function addDedupParamsToMessage (message, opt, messageOptions) {
  // Either of these means we need to calculate an id
  if (opt.fifo || opt.externalDedup) {
    const uuidFunction = opt.uuidFunction || uuidV1

    if (opt.deduplicationId) message.MessageDeduplicationId = opt.deduplicationId
    if (opt.dedupIdPerMessage) message.MessageDeduplicationId = uuidFunction()
    if (messageOptions?.deduplicationId) message.MessageDeduplicationId = messageOptions.deduplicationId

    // Fallback to using the message body
    if (!message.MessageDeduplicationId) {
      message.MessageDeduplicationId = getDeduplicationId(message.MessageBody, opt)
    } else {
      // Transform the incoming ID so that it fits SPS spec and is suitable for Redis
      message.MessageDeduplicationId = getDeduplicationId(message.MessageDeduplicationId, opt)
    }

    // Track our own dedup id so we can look it up upon ReceiveMessage
    if (opt.externalDedup) {
      message.MessageAttributes = {
        ...message.MessageAttributes,
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
  const now = Math.floor(Date.now() / 1000)

  // Grab a batch of expired keys
  debug({ statMaintenance: { aboutToGo: true, expirationSet } })
  const expiredStats = await client.zrange(expirationSet, '-inf', now, 'BYSCORE')
  debug({ statMaintenance: { expiredStats } })

  // And remove them from indexes, main storage
  if (expiredStats.length) {
    const result = await client.multi()
      .zrem(expirationSet, expiredStats)
      .zrem(duplicateSet, expiredStats)
      .exec()
    debug({ statMaintenance: { result } })
  }
}

export function messageDedupId (message) {
  return message?.MessageAttributes?.QdoneDeduplicationId?.StringValue
}

function messageOwner (message) {
  const attributes = message?.MessageAttributes
  if (!attributes || !Object.prototype.hasOwnProperty.call(attributes, 'QdoneDeduplicationOwner')) return ''
  const { DataType, StringValue } = attributes.QdoneDeduplicationOwner ?? {}
  return DataType === 'String' && typeof StringValue === 'string' && /^[0-9a-f]{32}$/.test(StringValue)
    ? StringValue
    : 'invalid'
}

export async function claimDedup (message, opt) {
  const cacheKey = getCacheKey(messageDedupId(message), opt)
  const { reply, token } = await claimOwner(getCacheClient(opt), cacheKey, opt)
  const [status, value, count, ttl] = reply
  switch (status) {
    case 'claimed':
      if (token) {
        message.MessageAttributes.QdoneDeduplicationOwner = { DataType: 'String', StringValue: token }
      }
      return { status }
    case 'legacyDeduplicated':
    case 'deduplicated':
      if (status === 'deduplicated' && (typeof value !== 'string' || !value.trim())) {
        return { status: 'unknown', reason: 'cache-response' }
      }
      if (opt.dedupStats) {
        const legacy = status === 'legacyDeduplicated'
        const expiry = Math.floor(Date.now() / 1000) + (legacy ? opt.dedupPeriod : Math.floor(ttl / 1000))
        try {
          await updateStats(cacheKey, legacy ? value : count, expiry, opt)
        } catch {
          debug('Duplicate statistics update failed')
        }
      }
      return status === 'deduplicated' ? { status, MessageId: value } : { status }
    case 'unknown':
      return { status, reason: value }
    default:
      throw new Error('Unexpected deduplication claim response')
  }
}

export async function dedupShouldEnqueue (message, opt) {
  const outcome = await claimDedup(message, opt)
  if (outcome.status === 'unknown') throw new Error('Deduplication acceptance is unknown')
  return outcome.status === 'claimed'
}

export async function dedupShouldEnqueueMulti (messages, opt) {
  const admitted = await Promise.all(messages.map(message => dedupShouldEnqueue(message, opt)))
  return messages.filter((message, index) => admitted[index])
}

export async function confirmDedup (message, messageId, opt) {
  const owner = messageOwner(message)
  if (owner) {
    return confirmOwner(getCacheClient(opt), getCacheKey(messageDedupId(message), opt), owner, messageId)
  }
}

export async function releasePendingDedup (message, opt) {
  const owner = messageOwner(message)
  if (owner) {
    return releaseOwner(getCacheClient(opt), getCacheKey(messageDedupId(message), opt), owner, true)
  }
}

export async function dedupSuccessfullyProcessed (message, opt) {
  const dedupId = messageDedupId(message)
  if (!dedupId) return 0
  const count = await releaseOwner(getCacheClient(opt), getCacheKey(dedupId, opt), messageOwner(message), false)
  if (opt.dedupStats && Math.random() < 1 / 100) await statMaintenance(opt)
  return count
}

export async function dedupSuccessfullyProcessedMulti (messages, opt) {
  const items = messages.filter(message => messageDedupId(message))
  if (!items.length) return 0
  const client = getCacheClient(opt)
  const release = (target, message) => releaseOwner(
    target, getCacheKey(messageDedupId(message), opt), messageOwner(message), false
  )
  let counts
  if (client.isCluster) {
    // ioredis requires all commands in an explicit pipeline to route to one node.
    counts = await Promise.all(items.map(message => release(client, message)))
  } else {
    const pipeline = client.pipeline()
    for (const message of items) release(pipeline, message)
    counts = (await pipeline.exec()).map(([error, deleted]) => {
      if (error) throw error
      return deleted
    })
  }
  const count = counts.reduce((sum, deleted) => sum + deleted, 0)
  if (opt.dedupStats && Math.random() < count / 100) await statMaintenance(opt)
  return count
}
