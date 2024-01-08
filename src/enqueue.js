import { addBreadcrumb } from '@sentry/node'
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
  normalizeQueueName,
  normalizeFailQueueName,
  normalizeDLQName
} from './qrlCache.js'
import { getSQSClient } from './sqs.js'
import { getOptionsWithDefaults } from './defaults.js'
import { ExponentialBackoff } from './exponentialBackoff.js'

const debug = Debug('qdone:enqueue')

export async function getOrCreateDLQ (queue, opt) {
  debug('getOrCreateDLQ(', queue, ')')
  const dqname = normalizeDLQName(queue, opt)
  try {
    const dqrl = await qrlCacheGet(dqname)
    return dqrl
  } catch (err) {
    // Anything other than queue doesn't exist gets re-thrown
    if (!(err instanceof QueueDoesNotExist)) throw err

    // Create our DLQ
    const client = getSQSClient()
    const params = {
      Attributes: { MessageRetentionPeriod: opt.messageRetentionPeriod + '' },
      QueueName: dqname
    }
    if (opt.tags) params.tags = opt.tags
    if (opt.fifo) params.Attributes.FifoQueue = 'true'
    const cmd = new CreateQueueCommand(params)
    if (opt.verbose) console.error(chalk.blue('Creating dead letter queue ') + dqname)
    const data = await client.send(cmd)
    debug('createQueue returned', data)
    const dqrl = data.QueueUrl
    qrlCacheSet(dqname, dqrl)
    return dqrl
  }
}

export async function getOrCreateFailQueue (queue, opt) {
  debug('getOrCreateFailQueue(', queue, ')')
  const fqname = normalizeFailQueueName(queue, opt)
  try {
    const fqrl = await qrlCacheGet(fqname)
    return fqrl
  } catch (err) {
    // Anything other than queue doesn't exist gets re-thrown
    if (!(err instanceof QueueDoesNotExist)) throw err

    // Crate our fail queue
    const client = getSQSClient()
    const params = {
      Attributes: { MessageRetentionPeriod: opt.messageRetentionPeriod + '' },
      QueueName: fqname
    }
    // If we have a dlq, we grab it and set a redrive policy
    if (opt.dlq) {
      const dqrl = await getOrCreateDLQ(queue, opt)
      const dqa = await getQueueAttributes(dqrl)
      debug('dqa', dqa)
      params.Attributes.RedrivePolicy = JSON.stringify({
        deadLetterTargetArn: dqa.Attributes.QueueArn,
        maxReceiveCount: opt.dlqAfter + ''
      })
    }
    if (opt.failDelay) params.Attributes.DelaySeconds = opt.failDelay
    if (opt.tags) params.tags = opt.tags
    if (opt.fifo) params.Attributes.FifoQueue = 'true'
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

    // Get our fail queue so we can create our own
    const fqrl = await getOrCreateFailQueue(qname, opt)
    const fqa = await getQueueAttributes(fqrl)

    // Create our queue
    const client = getSQSClient()
    const params = {
      Attributes: {
        MessageRetentionPeriod: opt.messageRetentionPeriod + '',
        RedrivePolicy: JSON.stringify({
          deadLetterTargetArn: fqa.Attributes.QueueArn,
          maxReceiveCount: '1'
        })
      },
      QueueName: qname
    }
    if (opt.tags) params.tags = opt.tags
    if (opt.fifo) params.Attributes.FifoQueue = 'true'
    const cmd = new CreateQueueCommand(params)
    debug({ params })
    if (opt.verbose) console.error(chalk.blue('Creating queue ') + qname)
    const data = await client.send(cmd)
    debug('createQueue returned', data)
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

export function formatMessage (command, id) {
  const message = {
    /*
    MessageAttributes: {
      City: { DataType: 'String', StringValue: 'Any City' },
      Population: { DataType: 'Number', StringValue: '1250800' }
    },
    */
    MessageBody: command
  }
  if (typeof id !== 'undefined') message.Id = '' + id
  return message
}

// Retry happens within the context of the send functions
const retryableExceptions = [
  RequestThrottled,
  KmsThrottled,
  QueueDoesNotExist // Queue could temporarily not exist due to eventual consistency, let it retry
]

export async function sendMessage (qrl, command, opt) {
  debug('sendMessage(', qrl, command, ')')
  const params = Object.assign({ QueueUrl: qrl }, formatMessage(command))
  if (opt.fifo) {
    params.MessageGroupId = opt.groupId
    params.MessageDeduplicationId = opt.deduplicationId || uuidV1()
  }
  if (opt.delay) params.DelaySeconds = opt.delay

  // Send it
  const client = getSQSClient()
  const cmd = new SendMessageCommand(params)
  debug({ cmd })
  const backoff = new ExponentialBackoff(opt.sendRetries)
  const send = async (attemptNumber) => {
    cmd.input.attemptNumber = attemptNumber
    const data = await client.send(cmd)
    debug('sendMessage returned', data)
    return data
  }
  const shouldRetry = async (result, error) => {
    for (const exceptionClass of retryableExceptions) {
      if (error instanceof exceptionClass) {
        debug({ sendMessageRetryingBecause: { error, result } })
        return true
      }
    }
    return false
  }
  const result = await backoff.run(send, shouldRetry)
  debug({ sendMessageResult: result })
  return result
}

export async function sendMessageBatch (qrl, messages, opt) {
  debug('sendMessageBatch(', qrl, messages.map(e => Object.assign(Object.assign({}, e), { MessageBody: e.MessageBody.slice(0, 10) + '...' })), ')')
  const params = { Entries: messages, QueueUrl: qrl }
  const uuidFunction = opt.uuidFunction || uuidV1
  // Add in group id if we're using fifo
  if (opt.fifo) {
    params.Entries = params.Entries.map(
      message => Object.assign({
        MessageGroupId: opt.groupIdPerMessage ? uuidFunction() : opt.groupId,
        MessageDeduplicationId: uuidFunction()
      }, message)
    )
  }
  if (opt.delay) {
    params.Entries = params.Entries.map(message =>
      Object.assign({ DelaySeconds: opt.delay }, message))
  }
  if (opt.sentryDsn) {
    addBreadcrumb({ category: 'sendMessageBatch', message: JSON.stringify({ params }), level: 'debug' })
  }
  debug({ params })

  // Send them
  const client = getSQSClient()
  const cmd = new SendMessageBatchCommand(params)
  debug({ cmd })
  const backoff = new ExponentialBackoff(opt.sendRetries)
  const send = async (attemptNumber) => {
    debug({ sendMessageBatchSend: { attemptNumber, params } })
    const data = await client.send(cmd)
    return data
  }
  const shouldRetry = (result, error) => {
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

const messages = {}
let requestCount = 0

//
// Flushes the internal message buffer for qrl.
// If the message is too large, batch is retried with half the messages.
// Returns number of messages flushed.
//
export async function flushMessages (qrl, opt) {
  debug('flushMessages', qrl)
  // Flush until empty
  let numFlushed = 0
  async function whileNotEmpty () {
    if (!(messages[qrl] && messages[qrl].length)) return numFlushed
    // Construct batch until full
    const batch = []
    let nextSize = JSON.stringify(messages[qrl][0]).length
    let totalSize = 0
    while ((totalSize + nextSize) < 262144 && messages[qrl].length && batch.length < 10) {
      batch.push(messages[qrl].shift())
      totalSize += nextSize
      if (messages[qrl].length) nextSize = JSON.stringify(messages[qrl][0]).length
      else nextSize = 0
    }

    // Send batch
    const data = await sendMessageBatch(qrl, batch, opt)
    debug({ data })

    // Fail if there are any individual message failures
    if (data.Failed && data.Failed.length) {
      const err = new Error('One or more message failures: ' + JSON.stringify(data.Failed))
      err.Failed = data.Failed
      throw err
    }

    // If we actually managed to flush any of them
    if (batch.length) {
      requestCount += 1
      data.Successful.forEach(message => {
        if (opt.verbose) console.error(chalk.blue('Enqueued job ') + message.MessageId + chalk.blue(' request ' + requestCount))
      })
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
let messageIndex = 0
export async function addMessage (qrl, command, opt) {
  const message = formatMessage(command, messageIndex++)
  messages[qrl] = messages[qrl] || []
  messages[qrl].push(message)
  debug({ location: 'addMessage', messages })
  if (messages[qrl].length >= 10) {
    return flushMessages(qrl, opt)
  }
  return 0
}

//
// Enqueue a single command
// Returns a promise for the SQS API response.
//
export async function enqueue (queue, command, options) {
  debug('enqueue(', { queue, command }, ')')
  const opt = getOptionsWithDefaults(options)
  const qrl = await getOrCreateQueue(queue, opt)
  return sendMessage(qrl, command, opt)
}

//
// Enqueue many commands formatted as an array of {queue: ..., command: ...} pairs.
// Returns a promise for the total number of messages enqueued.
//
export async function enqueueBatch (pairs, options) {
  debug('enqueueBatch(', pairs, ')')
  const opt = getOptionsWithDefaults(options)

  // Find unique queues so we can pre-fetch qrls. We do this so that all
  // queues are created prior to going through our flush logic
  const normalizedPairs = pairs.map(({ queue, command }) => ({
    qname: normalizeQueueName(queue, opt),
    command
  }))
  const uniqueQnames = new Set(normalizedPairs.map(p => p.qname))

  // Prefetch qrls / create queues in parallel
  const createPromises = []
  for (const qname of uniqueQnames) {
    createPromises.push(getOrCreateQueue(qname, opt))
  }
  await Promise.all(createPromises)

  // After we've prefetched, all qrls are in cache
  // so go back through the list of pairs and fire off messages
  requestCount = 0
  let initialFlushTotal = 0
  for (const { qname, command } of normalizedPairs) {
    const qrl = await getOrCreateQueue(qname, opt)
    initialFlushTotal += await addMessage(qrl, command, opt)
  }

  // And flush any remaining messages
  const extraFlushPromises = []
  for (const qrl in messages) {
    extraFlushPromises.push(flushMessages(qrl, opt))
  }
  const extraFlushCounts = await Promise.all(extraFlushPromises)
  const extraFlushTotal = extraFlushCounts.reduce((a, b) => a + b, 0)
  const totalFlushed = initialFlushTotal + extraFlushTotal
  debug({ initialFlushTotal, extraFlushTotal, totalFlushed })
  return totalFlushed
}

debug('loaded')
