import { addBreadcrumb, setExtra } from '@sentry/node'
import { v1 as uuidV1 } from 'uuid'
import chalk from 'chalk'
import Debug from 'debug'
import {
  CreateQueueCommand,
  GetQueueAttributesCommand,
  SendMessageCommand,
  SendMessageBatchCommand,
  QueueDoesNotExist,
  RequestThrottled,
  KmsThrottled
} from '@aws-sdk/client-sqs'

import {
  qrlCacheGet,
  qrlCacheSet,
  qrlCacheInvalidate,
  normalizeQueueName,
  normalizeFailQueueName,
  normalizeDLQName
} from './qrlCache.js'
import { getSQSClient } from './sqs.js'
import {
  addDedupParamsToMessage,
  dedupShouldEnqueue,
  dedupSuccessfullyProcessed
} from './dedup.js'
import { getOptionsWithDefaults, validateMessageOptions } from './defaults.js'
import { ExponentialBackoff } from './exponentialBackoff.js'

const debug = Debug('qdone:enqueue')

export function getDLQParams (queue, opt) {
  const dqname = normalizeDLQName(queue, opt)
  const params = {
    Attributes: { MessageRetentionPeriod: opt.messageRetentionPeriod + '' },
    QueueName: dqname
  }
  if (opt.tags) params.tags = opt.tags
  if (opt.fifo) params.Attributes.FifoQueue = 'true'
  return { dqname, params }
}

export async function getOrCreateDLQ (queue, opt) {
  debug('getOrCreateDLQ(', queue, ')')
  const { dqname, params } = getDLQParams(queue, opt)
  try {
    const dqrl = await qrlCacheGet(dqname)
    return dqrl
  } catch (err) {
    // Anything other than queue doesn't exist gets re-thrown
    if (!(err instanceof QueueDoesNotExist)) throw err

    // Create our DLQ
    const client = getSQSClient()
    const cmd = new CreateQueueCommand(params)
    if (opt.verbose) console.error(chalk.blue('Creating dead letter queue ') + dqname)
    const data = await client.send(cmd)
    debug('createQueue returned', data)
    const dqrl = data.QueueUrl
    qrlCacheSet(dqname, dqrl)
    return dqrl
  }
}

/**
 * Returns the parameters needed for creating a failed queue. If DLQ options
 * are set, it makes an API call to get this DLQ's ARN.
 */
export async function getFailParams (queue, opt) {
  const fqname = normalizeFailQueueName(queue, opt)
  const params = {
    Attributes: { MessageRetentionPeriod: opt.messageRetentionPeriod + '' },
    QueueName: fqname
  }
  // If we have a dlq, we grab it and set a redrive policy
  if (opt.dlq) {
    const dqname = normalizeDLQName(queue, opt)
    const dqrl = await qrlCacheGet(dqname)
    const dqa = await getQueueAttributes(dqrl)
    debug('dqa', dqa)
    params.Attributes.RedrivePolicy = JSON.stringify({
      deadLetterTargetArn: dqa.Attributes.QueueArn,
      maxReceiveCount: opt.dlqAfter
    })
  }
  if (opt.failDelay) params.Attributes.DelaySeconds = opt.failDelay + ''
  if (opt.tags) params.tags = opt.tags
  if (opt.fifo) params.Attributes.FifoQueue = 'true'
  return params
}

/**
 * Returns the qrl for the failed queue for the given queue. Creates the queue
 * if it does not exist.
 */
export async function getOrCreateFailQueue (queue, opt, doesNotExist) {
  debug('getOrCreateFailQueue(', queue, ')')
  const fqname = normalizeFailQueueName(queue, opt)
  try {
    // Bail early if the caller knew we didn't have a queue
    if (doesNotExist) throw new QueueDoesNotExist(fqname)
    const fqrl = await qrlCacheGet(fqname)
    return fqrl
  } catch (err) {
    // Anything other than queue doesn't exist gets re-thrown
    if (!(err instanceof QueueDoesNotExist)) throw err

    // Grab params, creating DLQ if needed
    let params
    try {
      params = await getFailParams(queue, opt)
    } catch (e) {
      // If DLQ doesn't exist, create it
      if (!(opt.dlq && e instanceof QueueDoesNotExist)) throw e
      await getOrCreateDLQ(queue, opt)
      params = await getFailParams(queue, opt)
    }

    // Create our fail queue
    const client = getSQSClient()
    const cmd = new CreateQueueCommand(params)
    if (opt.verbose) console.error(chalk.blue('Creating fail queue ') + fqname)
    const data = await client.send(cmd)
    debug('createQueue returned', data)
    const fqrl = data.QueueUrl
    qrlCacheSet(fqname, fqrl)
    return fqrl
  }
}

/**
 * Returns the parameters needed for creating a queue. If fail options
 * are set, it makes an API call to get the fail queue's ARN.
 */
export async function getQueueParams (queue, opt) {
  const qname = normalizeQueueName(queue, opt)
  const fqname = normalizeFailQueueName(queue, opt)
  const fqrl = await qrlCacheGet(fqname, opt)
  const fqa = await getQueueAttributes(fqrl)
  const params = {
    Attributes: {
      MessageRetentionPeriod: opt.messageRetentionPeriod + '',
      RedrivePolicy: JSON.stringify({
        deadLetterTargetArn: fqa.Attributes.QueueArn,
        maxReceiveCount: 1
      })
    },
    QueueName: qname
  }
  if (opt.tags) params.tags = opt.tags
  if (opt.fifo) params.Attributes.FifoQueue = 'true'
  return params
}

/**
 * Returns a qrl for a queue that either exists or does not
 */
export async function getOrCreateQueue (queue, opt) {
  debug('getOrCreateQueue(', queue, ')')
  const qname = normalizeQueueName(queue, opt)
  try {
    const qrl = await qrlCacheGet(qname)
    return qrl
  } catch (err) {
    // Anything other than queue doesn't exist gets re-thrown
    if (!(err instanceof QueueDoesNotExist)) throw err

    // Grab params, creating fail queue if needed
    let params
    try {
      params = await getQueueParams(qname, opt)
    } catch (e) {
      // If fail queue doesn't exist, create it
      if (!(e instanceof QueueDoesNotExist)) throw e
      await getOrCreateFailQueue(qname, opt, true)
      params = await getQueueParams(qname, opt)
    }

    debug({ getOrCreateQueue: { qname, params } })

    // Create our queue
    const client = getSQSClient()
    const cmd = new CreateQueueCommand(params)
    if (opt.verbose) console.error(chalk.blue('Creating fail queue ') + qname)
    const data = await client.send(cmd)
    debug('AWS createQueue returned', data)
    const qrl = data.QueueUrl
    qrlCacheSet(qname, qrl)
    return qrl
  }
}

export async function getQueueAttributes (qrl) {
  debug('getQueueAttributes(', qrl, ')')
  const client = getSQSClient()
  const params = { AttributeNames: ['All'], QueueUrl: qrl }
  const cmd = new GetQueueAttributesCommand(params)
  // debug({ cmd })
  const data = await client.send(cmd)
  debug('GetQueueAttributes returned', data)
  return data
}

export function formatMessage (body, id, opt, messageOptions) {
  const message = { MessageBody: body }
  if (typeof id !== 'undefined') message.Id = '' + id
  if (opt.fifo) {
    message.MessageGroupId = messageOptions?.groupId || opt?.groupId
  }
  addDedupParamsToMessage(message, opt, messageOptions)
  if (opt.delay) message.DelaySeconds = opt.delay
  if (messageOptions?.delay) message.DelaySeconds = messageOptions.delay
  return message
}

// Retry happens within the context of the send functions
const retryableExceptions = [
  RequestThrottled,
  KmsThrottled,
  QueueDoesNotExist // Queue could temporarily not exist due to eventual consistency, let it retry
]

export async function sendMessage (qrl, queue, command, opt, messageOptions) {
  debug('sendMessage(', qrl, command, ')')
  const uuidFunction = opt.uuidFunction || uuidV1
  const params = {
    QueueUrl: qrl,
    ...formatMessage(command, null, opt, messageOptions)
  }

  // See if we even have to send it
  if (opt.externalDedup) {
    const shouldEnqueue = await dedupShouldEnqueue(params, opt)
    if (!shouldEnqueue) return { MessageId: uuidFunction() }
  }

  // Send it
  const client = getSQSClient()
  let cmd = new SendMessageCommand(params)
  debug({ cmd })
  const backoff = new ExponentialBackoff(opt.sendRetries)
  const send = async (attemptNumber) => {
    cmd.input.attemptNumber = attemptNumber
    const data = await client.send(cmd)
    debug('sendMessage returned', data)
    return data
  }
  const shouldRetry = async (result, error) => {
    if (!error) return false

    if (error instanceof QueueDoesNotExist) {
      const qname = normalizeQueueName(queue, opt)
      qrlCacheInvalidate(qname)
      // clear cache in case cache does not reflect reality, then try recreating the queue before sending message again
      const newQrl = await getOrCreateQueue(queue, opt)
      params.QueueUrl = newQrl
      cmd = new SendMessageCommand(params)
    }

    for (const exceptionClass of retryableExceptions) {
      if (error instanceof exceptionClass) {
        debug({ sendMessageRetryingBecause: { error, result } })
        return true
      }
    }
    // If we could not send it, we also need to remove our dedup flag
    await dedupSuccessfullyProcessed(params, opt)
    return false
  }
  const result = await backoff.run(send, shouldRetry)
  debug({ sendMessageResult: result })
  return result
}

export async function sendMessageBatch (qrl, queue, messages, opt) {
  debug('sendMessageBatch(', qrl, messages.map(e => Object.assign(Object.assign({}, e), { MessageBody: e.MessageBody.slice(0, 10) + '...' })), ')')
  const params = { Entries: messages, QueueUrl: qrl }
  if (opt.sentryDsn) {
    addBreadcrumb({ category: 'sendMessageBatch', message: JSON.stringify({ params }), level: 'debug' })
  }
  debug({ params })

  // See which messages we even have to send
  if (opt.externalDedup) {
    const promises = params.Entries.map(async m => ({ m, shouldEnqueue: await dedupShouldEnqueue(m, opt) }))
    const results = await Promise.all(promises)
    params.Entries = results.filter(({ shouldEnqueue }) => shouldEnqueue).map(({ m }) => m)
    if (!params.Entries.length) {
      const result = {
        Failed: [],
        Successful: results.map(
          ({ m: { Id: id, MessageAttributes: ma } }) => ({
            Id: id,
            MessageId: 'duplicate',
            QdoneDeduplicationId: ma?.QdoneDeduplicationId?.StringValue
          }))
      }
      return result
    }
  }

  // Send them
  const client = getSQSClient()
  let cmd = new SendMessageBatchCommand(params)
  debug({ cmd })
  const backoff = new ExponentialBackoff(opt.sendRetries)
  const send = async (attemptNumber) => {
    debug({ sendMessageBatchSend: { attemptNumber, params } })
    const data = await client.send(cmd)
    return data
  }
  const shouldRetry = async (result, error) => {
    debug({ shouldRetry: { error, result } })
    if (result) {
      // Handle failed result of one or more messages in the batch
      if (result.Failed && result.Failed.length) {
        for (const failed of result.Failed) {
          // Find corresponding messages
          const original = params.Entries.find((e) => e.Id === failed.Id)
          const info = { failed, original, opt }
          if (opt.sentryDsn) {
            addBreadcrumb({ category: 'sendMessageBatch', message: 'Failed message: ' + JSON.stringify(info), level: 'error' })
          } else {
            console.error(info)
          }
        }
        throw new Error('One or more message failures: ' + JSON.stringify(result.Failed))
      }
    }
    if (error) {
      // Handle a failed result from an overall error on request
      if (opt.sentryDsn) {
        addBreadcrumb({ category: 'sendMessageBatch', message: JSON.stringify({ error }), level: 'error' })
      }

      if (error instanceof QueueDoesNotExist) {
        const qname = normalizeQueueName(queue, opt)
        qrlCacheInvalidate(qname)
        // Clear stale cache entry and recreate queue before retrying
        const newQrl = await getOrCreateQueue(queue, opt)
        params.QueueUrl = newQrl
        cmd = new SendMessageBatchCommand(params)
      }

      for (const exceptionClass of retryableExceptions) {
        debug({ exceptionClass, retryableExceptions })
        if (error instanceof exceptionClass) {
          debug({ sendMessageRetryingBecause: { error, result } })
          return true
        }
      }
    }
  }
  return backoff.run(send, shouldRetry)
}

let requestCount = 0

//
// Flushes the internal message buffer for qrl.
// If the message is too large, batch is retried with half the messages.
// Returns number of messages flushed.
//
export async function flushMessages (qrl, queue, opt, sendBuffer) {
  debug('flushMessages', { qrl, queue, sendBuffer })
  // Track our outgoing messages to map with Failed / Successful returns
  const messagesById = new Map()
  const resultsById = new Map()
  const results = []
  if (sendBuffer[qrl] && sendBuffer[qrl].length) {
    for (const message of sendBuffer[qrl]) {
      const Id = message.Id
      messagesById.set(Id, message)
      // Pre-prepare results
      const result = { Id }
      resultsById.set(Id, result)
      results.push(result)
    }
  }
  // Flush until empty
  let numFlushed = 0
  async function whileNotEmpty () {
    if (!(sendBuffer[qrl] && sendBuffer[qrl].length)) {
      return { numFlushed, results }
    }
    // Construct batch until full
    const batch = []
    let nextSize = JSON.stringify(sendBuffer[qrl][0]).length
    let totalSize = 0
    while ((totalSize + nextSize) < 262144 && sendBuffer[qrl].length && batch.length < 10) {
      batch.push(sendBuffer[qrl].shift())
      totalSize += nextSize
      if (sendBuffer[qrl].length) nextSize = JSON.stringify(sendBuffer[qrl][0]).length
      else nextSize = 0
    }

    // Send batch
    const data = await sendMessageBatch(qrl, queue, batch, opt)

    // Fail if there are any individual message failures
    if (data?.Failed && data?.Failed.length) {
      const err = new Error('One or more message failures: ' + JSON.stringify(data.Failed))
      err.Failed = data.Failed
      throw err
    }

    // If we actually managed to flush any of them
    if (batch.length) {
      requestCount += 1
      if (data?.Successful) {
        for (const { Id, MessageId } of data.Successful) {
          const result = resultsById.get(Id)
          const message = messagesById.get(Id)
          result.MessageId = MessageId
          result.Id = Id
          if (message?.MessageAttributes?.QdoneDeduplicationId?.StringValue) {
            result.QdoneDeduplicationId = message?.MessageAttributes?.QdoneDeduplicationId?.StringValue
          }
          if (opt.verbose) console.error(chalk.blue('Enqueued job ') + MessageId + chalk.blue(' request ' + requestCount))
        }
      }
      numFlushed += batch.length
    }
    return whileNotEmpty()
  }
  return whileNotEmpty()
}

//
// Adds a message to the inernal message buffer for the given qrl.
// Automaticaly flushes if queue has >= 10 messages.
// Returns number of messages flushed.
//
const debugAddMessage = Debug('qdone:enqueue:addMessage')
export async function addMessage (qrl, queue, command, messageIndex, opt, sendBuffer, messageOptions) {
  const message = formatMessage(command, messageIndex, opt, messageOptions)
  sendBuffer[qrl] = sendBuffer[qrl] || []
  sendBuffer[qrl].push(message)
  debugAddMessage({ location: 'addMessage', messageIndex, sendBuffer })
  if (sendBuffer[qrl].length >= 10) {
    return flushMessages(qrl, queue, opt, sendBuffer)
  }
  return { numFlushed: 0, results: [] }
}

//
// Enqueue a single command
// Returns a promise for the SQS API response.
//
export async function enqueue (queue, command, options) {
  debug('enqueue(', { queue, command }, ')')
  const opt = getOptionsWithDefaults(options)
  if (opt.sentryDsn) {
    setExtra({ qdoneOperation: 'enqueue', args: { queue, command, opt } })
  }
  try {
    const qrl = await getOrCreateQueue(queue, opt)
    return sendMessage(qrl, queue, command, opt)
  } catch (e) {
    console.log(e)
    throw e
  }
}

//
// Enqueue many commands formatted as an array of {queue: ..., command: ...} pairs.
// Returns a promise for the total number of messages enqueued.
//
export async function enqueueBatch (pairs, options) {
  debug('enqueueBatch(', pairs, ')')
  const opt = getOptionsWithDefaults(options)
  if (opt.sentryDsn) {
    setExtra({ qdoneOperation: 'enqueueBatch', args: { pairs, opt } })
  }
  try {
    const allResults = []
    // Find unique queues so we can pre-fetch qrls. We do this so that all
    // queues are created prior to going through our flush logic
    const normalizedPairs = pairs.map(({ queue, command, messageOptions }) => ({
      qname: normalizeQueueName(queue, opt),
      command,
      messageOptions: validateMessageOptions(messageOptions)
    }))
    const uniqueQnames = new Set(normalizedPairs.map(p => p.qname))

    // Prefetch qrls / create queues in parallel, building a reverse map for cache invalidation
    const createPromises = []
    const qrlToQname = new Map()
    for (const qname of uniqueQnames) {
      createPromises.push(
        getOrCreateQueue(qname, opt).then(qrl => qrlToQname.set(qrl, qname))
      )
    }
    await Promise.all(createPromises)
    // After we've prefetched, all qrls are in cache
    // so go back through the list of pairs and fire off messages
    requestCount = 0
    const sendBuffer = {}
    let messageIndex = 0
    let initialFlushTotal = 0
    for (const { qname, command, messageOptions } of normalizedPairs) {
      const qrl = await getOrCreateQueue(qname, opt)
      const { numFlushed, results } = await addMessage(qrl, qname, command, messageIndex++, opt, sendBuffer, messageOptions)
      initialFlushTotal += numFlushed
      allResults.push(...results)
    }

    // And flush any remaining messages
    const extraFlushPromises = []
    for (const qrl in sendBuffer) {
      extraFlushPromises.push(flushMessages(qrl, qrlToQname.get(qrl), opt, sendBuffer))
    }
    let extraFlushTotal = 0
    for (const { numFlushed, results } of await Promise.all(extraFlushPromises)) {
      allResults.push(...results)
      extraFlushTotal += numFlushed
    }
    const totalFlushed = initialFlushTotal + extraFlushTotal
    debug({ initialFlushTotal, extraFlushTotal, totalFlushed })
    return { numFlushed: totalFlushed, results: allResults }
  } catch (e) {
    console.log(e)
    throw e
  }
}

debug('loaded')
