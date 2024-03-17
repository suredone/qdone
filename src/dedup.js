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
  if (opt.fifo) {
    const uuidFunction = opt.uuidFunction || uuidV1
    if (opt.deduplicationId) message.MessageDeduplicationId = opt.deduplicationId
    if (opt.dedupIdPerMessage) message.MessageDeduplicationId = uuidFunction()

    // Fallback to using the message body
    if (!message.MessageDeduplicationId) {
      message.MessageDeduplicationId = getDeduplicationId(message.MessageBody, opt)
    }

    // Track this so we can see it on the receiving end
    if (opt.externalDedup) {
      message.MessageAttributes = {
        QdoneDeduplicationId: {
          StringValue: message.MessageDeduplicationId,
          DataType: 'String'
        }
      }
    }
  }
  return message
}

/**
 * Calculates:
 *  - dedupPeriod: the number of seconds from now until another duplicate is
 *    allowed to run (if the current running duplicate does not finish earlier)
 *  - canExpireAfter: the earliest absolute timestamp when another duplicate
 *    job can run provided the running job completes. This value is used by
 *    dedupSuccessfullyProcessed and dedupSuccessfullyProcessedMulti to either
 *    expire a the key or schedule it's expiration if a job finishes
 */
export function calculateDedupParams (opt) {
  // We won, now make sure it expires
  const minDedupPeriod = 6 * 60
  const dedupPeriod = Math.max(opt.dedupPeriod, minDedupPeriod) // at least minDedupPeriod
  const timestamp = new Date().getTime()
  // Wait at least the minDedupPeriod before we are allowed to expire
  const canExpireAfter = Math.round(timestamp / 1000.0 + minDedupPeriod)
  debug({ calculateDedupParams: { minDedupPeriod, dedupPeriod, timestamp, canExpireAfter }})
  return { minDedupPeriod, dedupPeriod, timestamp, canExpireAfter }
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
  const result = await client.incr(cacheKey)
  debug({ action: 'shouldEnqueue', cacheKey, result })
  if (result === 1) {
    const { canExpireAfter, dedupPeriod } = calculateDedupParams(opt)
    await client.set(cacheKey, canExpireAfter, 'EX', dedupPeriod)
    return true
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
  const messagesToEnqueue = []
  for (let i = 0; i < messages.length; i++) {
    const message = messages[i]
    const [, response] = responses[i]
    const dedupId = message?.MessageAttributes?.QdoneDeduplicationId?.StringValue
    const cacheKey = getCacheKey(dedupId, opt)
    if (response === 1) {
      messagesToEnqueue.push(message)
      const { canExpireAfter, dedupPeriod } = calculateDedupParams(opt)
      expirePipeline.set(cacheKey, canExpireAfter, 'EX', dedupPeriod)
    }
  }
  await expirePipeline.exec()
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
    // Instead of just deleting the key here, we need to make sure that we
    // wait at least as long as SQS's dedup period. This is because we may
    // get a successful call to enqueue a message that doesn't really send
    // the message because of SQS's own dedup. This can cause us to
    // accidentally refuse to enqueue a message in the following situation:
    //   1) Send a message. We check the dedup id and give the OK
    //   2) SQS gets the message, they track dedup id for 5 minutes
    //   3) We process message and delete it within the 5 minutes
    //   4) We also delete our record, allowing future duplicates
    //   5) We enqueue another duplicate, our record allows it
    //   6) We send to SQS and get a success, but SQS has silently dropped
    //      because we are still in the 5 minute window
    //   7) Now there is no message in flight to trigger deleting this key
    //      and there won't be until the end of the dedup period
    // So, the upshot is that we have to wait at least as long as SQS
    const cacheKey = getCacheKey(dedupId, opt)
    const canExpireAfter = parseInt(await client.get(cacheKey))
    // If canExpireAfter is in the past, this call will delete the key
    // otherwise it will delete as soon as the min dedup period is up
    debug({ dedupSuccessfullyProcessed: { dedupId, cacheKey, canExpireAfter } })
    // Note that if the key has already expired, we need do nothing
    if (canExpireAfter > 0) {
      const result = await client.expireat(cacheKey, canExpireAfter)
      debug({ dedupSuccessfullyProcessed: { dedupId, cacheKey, result } })
    }
  }
}

/**
 * If we got a non-retryable error from SQS, we need to remove our key so the
 * application can try again without waiting the entire dedupPeriod.
 * @param {Object} message - Return value from RecieveMessageCommand
 * @param {Object} opt - Opt object from getOptionsWithDefaults()
 * @returns {Number} 1 if a cache key was deleted, otherwise 0
 */
export async function dedupErrorBeforeAcknowledgement (message, opt) {
  const dedupId = message?.MessageAttributes?.QdoneDeduplicationId?.StringValue
  if (dedupId) {
    const client = getCacheClient(opt)
    const cacheKey = getCacheKey(dedupId, opt)
    const result = await client.del(cacheKey)
    debug({ dedupErrorBeforeAcknowledgement: { dedupId, result } })
  }
}

// TODO: rewrite dedupSuccessfullyProcessed