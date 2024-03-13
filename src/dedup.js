import { createHash } from 'crypto'
import { getCacheClient } from './cache.js'
import Debug from 'debug'
const debug = Debug('qdone:dedup')

/**
 * This function returns the cache key for deduplication checks for the given
 * message. If the message has a MessageDeduplicationId, that is used,
 * otherwise the MessageBody is used.
 * @param message - Parameters to SendMessageCommand
 * @param opt - Opt object from getOptionsWithDefaults()
 * @returns the cache key
 */
export function getCacheKey (message, opt) {
  let dedupContent = message?.MessageDeduplicationId || message?.MessageAttributes?.DeduplicationId?.StringValue || message?.MessageBody || message?.Body
  // Don't transmit long keys to redis
  const max = 256
  const sep = '...sha1:'
  if (dedupContent.length > max) {
    dedupContent = dedupContent.slice(0, max - sep.length - 40) + '...sha1:' + createHash('sha1').update(dedupContent).digest('hex')
  }
  const cacheKey = opt.cachePrefix + 'dedup:' + dedupContent.trim()
  debug({ action: 'shouldEnqueue', cacheKey })
  return cacheKey
}

/**
 * Determines whether we should enqueue this message or whether it is a duplicate.
 * Returns true if enqueuing the message would not result in a duplicate.
 * @param message - Parameters to SendMessageCommand
 * @param opt - Opt object from getOptionsWithDefaults()
 * @returns true if the message can be enqueued without duplicate, else false
 */
export async function dedupShouldEnqueue (message, opt) {
  if (opt.dedupMethod === 'redis') {
    const client = getCacheClient(opt)
    const cacheKey = getCacheKey(message, opt)
    const result = await client.incr(cacheKey)
    debug({ action: 'shouldEnqueue', cacheKey, result })
    if (result === 1) {
      // We won, now make sure it expires
      await client.expire(cacheKey, opt.dedupPeriod)
      return true
    }
    return false
  } else {
    return true
  }
}

/**
 * Determines which messages we should enqueue, returning only those that
 * would not be duplicates. 
 * @param messages - params.Entries for the SendMessageBatchCommand
 * @param opt - Opt object from getOptionsWithDefaults()
 * @returns an array of messages that can be safely enqueued. Could be empty.
 */
export async function dedupShouldEnqueueMulti (messages, opt) {
  debug({ dedupShouldEnqueueMulti: { messages, opt }})
  if (opt.dedupMethod === 'redis') {
    // Increment all
    const incrPipeline = getCacheClient(opt).pipeline()
    for (const message of messages) {
      const cacheKey = getCacheKey(message, opt)
      incrPipeline.incr(cacheKey)
    }
    const responses = await incrPipeline.exec()
    debug({ dedupShouldEnqueueMulti: responses })

    // Interpret responses and expire keys for races we won
    const expirePipeline = getCacheClient(opt).pipeline()
    const messagesToEnqueue = []
    for (let i = 0; i < messages.length; i++) {
      const message = messages[i]
      const [, response] = responses[i]
      const cacheKey = getCacheKey(message, opt)
      if (response === 1) {
        messagesToEnqueue.push(message)
        expirePipeline.expire(cacheKey, opt.dedupPeriod)
      }
    }
    await expirePipeline.exec()
    return messagesToEnqueue
  } else {
    // If we're not using redis, send everything
    return messages
  }
}

/**
 * Marks a message as processed so that subsequent calls to dedupShouldEnqueue
 * and dedupShouldEnqueueMulti will allow a message to be enqueued again
 * wihtout waiting for dedupPeriod to expire.
 * @param message - Return value from RecieveMessageCommand
 * @param opt - Opt object from getOptionsWithDefaults()
 * @returns 1 if a cache key was deleted, otherwise 0
 */
export async function dedupSuccessfullyProcessed (message, opt) {
  if (opt.dedupMethod === 'redis') {
    const client = getCacheClient(opt)
    const cacheKey = getCacheKey(message, opt)
    debug({ action: 'shouldProcess', cacheKey })
    return client.del(cacheKey)
  }
  return 0
}

/**
 * Marks a message as processed so that subsequent calls to dedupShouldEnqueue
 * and dedupShouldEnqueueMulti will allow a message to be enqueued again
 * wihtout waiting for dedupPeriod to expire.
 * @param messages - Array of return values from RecieveMessageCommand
 * @param opt - Opt object from getOptionsWithDefaults()
 * @returns the number of cache keys deleted
 */
export async function dedupSuccessfullyProcessedMulti (messages, opt) {
  if (opt.dedupMethod === 'redis') {
    const delPipeline = getCacheClient(opt).pipeline()
    for (const message of messages) {
      const cacheKey = getCacheKey(message, opt)
      debug({ dedupSuccessfullyProcessedMulti: { cacheKey } })
      delPipeline.del(cacheKey)
    }
    const results = await delPipeline.exec()
    return results.map(([, val]) => val).reduce((a, b) => a + b, 0)
  }
  return 0
}
