// const Q = require('q')
// const debug = require('debug')('qdone:enqueue')
// const chalk = require('chalk')
// const uuid = require('uuid')
// const qrlCache = require('./qrlCache')
// const AWS = require('aws-sdk')

import { v1 as uuidV1 } from 'uuid'
import chalk from 'chalk'
import Debug from 'debug'
import {
  CreateQueueCommand,
  GetQueueAttributesCommand,
  SendMessageCommand,
  SendMessageBatchCommand
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

const debug = Debug('qdone:enqueue')

export async function getOrCreateDLQ (queue, opt) {
  debug('getOrCreateDLQ(', queue, ')')
  const dqname = normalizeDLQName(queue, opt)
  try {
    const dqrl = await qrlCacheGet(dqname)
    return dqrl
  } catch (err) {
    // Anything other than queue doesn't exist gets re-thrown
    if (err.name !== 'QueueDoesNotExist') throw err

    // Create our DLQ
    const client = getSQSClient()
    const params = {
      Attributes: { MessageRetentionPeriod: opt.messageRetentionPeriod + '' },
      QueueName: dqname
    }
    if (opt.fifo) params.Attributes.FifoQueue = 'true'
    const cmd = new CreateQueueCommand(params)
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
  debug({ fqname })
  try {
    const fqrl = await qrlCacheGet(fqname)
    return fqrl
  } catch (err) {
    // Anything other than queue doesn't exist gets re-thrown
    if (err.name !== 'QueueDoesNotExist') throw err

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
    if (opt.fifo) params.Attributes.FifoQueue = 'true'
    const cmd = new CreateQueueCommand(params)
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
    debug('getOrCreateQueue return', qrl)
    return qrl
  } catch (err) {
    debug('getOrCreateQueue err', err)
    // Anything other than queue doesn't exist gets re-thrown
    if (err.Code !== 'AWS.SimpleQueueService.NonExistentQueue') throw err

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
    if (opt.fifo) params.Attributes.FifoQueue = 'true'
    const cmd = new CreateQueueCommand(params)
    debug({ params})
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

export async function sendMessage (qrl, command, options) {
  debug('sendMessage(', qrl, command, ')')
  const params = Object.assign({ QueueUrl: qrl }, formatMessage(command))
  // Add in group id if we're using fifo
  if (options.fifo) {
    params.MessageGroupId = options['group-id']
    params.MessageDeduplicationId = options['deduplication-id']
  }
  if (options.delay) params.DelaySeconds = options.delay
  const client = getSQSClient()
  const cmd = new SendMessageCommand(params)
  debug({ cmd })
  const data = await client.send(cmd)
  debug('sendMessage returned', data)
  return data
}

export async function sendMessageBatch (qrl, messages, options) {
  debug('sendMessageBatch(', qrl, messages.map(e => Object.assign(Object.assign({}, e), { MessageBody: e.MessageBody.slice(0, 10) + '...' })), ')')
  const params = { Entries: messages, QueueUrl: qrl }
  const uuidFunction = options.uuidFunction || uuidV1
  // Add in group id if we're using fifo
  if (options.fifo) {
    params.Entries = params.Entries.map(
      message => Object.assign({
        MessageGroupId: options['group-id-per-message'] ? uuidFunction() : options['group-id'],
        MessageDeduplicationId: uuidFunction()
      }, message)
    )
  }
  if (options.delay) {
    params.Entries = params.Entries.map(message =>
      Object.assign({ DelaySeconds: options.delay }, message))
  }
  const client = getSQSClient()
  const cmd = new SendMessageBatchCommand(params)
  debug({ cmd })
  const data = await client.send(cmd)
  debug('sendMessageBatch returned', data)
  return data
}

const messages = {}
let requestCount = 0

//
// Flushes the internal message buffer for qrl.
// If the message is too large, batch is retried with half the messages.
// Returns number of messages flushed.
//
export async function flushMessages (qrl, options) {
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
    const data = await sendMessageBatch(qrl, batch, options)
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
        if (options.verbose) console.error(chalk.blue('Enqueued job ') + message.MessageId + chalk.blue(' request ' + requestCount))
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
export async function addMessage (qrl, command, options) {
  const message = formatMessage(command, messageIndex++)
  messages[qrl] = messages[qrl] || []
  messages[qrl].push(message)
  if (messages[qrl].length >= 10) {
    return flushMessages(qrl, options)
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
  const addMessagePromises = []
  for (const { qname, command } of normalizedPairs) {
    const qrl = await getOrCreateQueue(qname, opt)
    addMessagePromises.push(addMessage(qrl, command, opt))
  }
  const flushCounts = await Promise.all(addMessagePromises)

  // Count up how many were flushed during add
  debug('flushCounts', flushCounts)
  const initialFlushTotal = flushCounts.reduce((a, b) => a + b, 0)

  // And flush any remaining messages
  const extraFlushPromises = []
  for (const qrl in messages) {
    extraFlushPromises.push(flushMessages(qrl, options))
  }
  const extraFlushCounts = await Promise.all(extraFlushPromises)
  const extraFlushTotal = extraFlushCounts.reduce((a, b) => a + b, 0)
  const totalFlushed = initialFlushTotal + extraFlushTotal
  debug({ initialFlushTotal, extraFlushTotal, totalFlushed })
  return totalFlushed
}

debug('loaded')
