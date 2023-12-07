// const Q = require('q')
// const debug = require('debug')('qdone:enqueue')
// const chalk = require('chalk')
// const uuid = require('uuid')
// const qrlCache = require('./qrlCache')
// const AWS = require('aws-sdk')

import { v1 as uuidV1 } from 'uuid'
import chalk from 'chalk'
import {
  qrlCacheGet,
  qrlCacheSet,
  normalizeQueueName,
  normalizeFailQueueName
} from './qrlCache.js'
import {
  CreateQueueCommand,
  GetQueueAttributesCommand,
  SendMessageCommand,
  SendMessageBatchCommand
} from '@aws-sdk/client-sqs'
import { getClient } from './sqs.js'
import Debug from 'debug'
const debug = Debug('qdone:enqueue')

export async function createFailQueue (fqueue, fqname, deadLetterTargetArn, options) {
  debug('createFailQueue(', fqueue, fqname, ')')
  const client = getClient()
  const params = {
    /* Attributes: {MessageRetentionPeriod: '259200', RedrivePolicy: '{"deadLetterTargetArn": "arn:aws:sqs:us-east-1:80398EXAMPLE:MyDeadLetterQueue", "maxReceiveCount": "1000"}'}, */
    Attributes: {},
    QueueName: fqname
  }
  if (options.fifo) params.Attributes.FifoQueue = 'true'
  const cmd = new CreateQueueCommand(params)
  debug({ cmd })
  const data = await client.send(cmd)
  debug('createQueue returned', data)
  const fqrl = data.QueueUrl
  qrlCacheSet(fqname, fqrl)
  return fqrl
}

export async function createQueue (queue, qname, deadLetterTargetArn, options) {
  debug('createQueue(', queue, qname, ')')
  const client = getClient()
  const maxReceiveCount = '1'
  const params = {
    Attributes: {
      // MessageRetentionPeriod: '259200',
      RedrivePolicy: JSON.stringify(
        { deadLetterTargetArn, maxReceiveCount }
      )
    },
    QueueName: qname
  }
  if (options.fifo) params.Attributes.FifoQueue = 'true'
  const cmd = new CreateQueueCommand(params)
  debug({ cmd })
  const data = await client.send(cmd)
  debug('createQueue returned', data)
  const qrl = data.QueueUrl
  qrlCacheSet(qname, qrl)
  return qrl
}

export async function getQueueAttributes (qrl) {
  debug('getQueueAttributes(', qrl, ')')
  const client = getClient()
  const params = { AttributeNames: ['All'], QueueUrl: qrl }
  const cmd = new GetQueueAttributesCommand(params)
  debug({ cmd })
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
  const client = getClient()
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
  const client = getClient()
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
// Fetches (or returns cached) the qrl
// Creates queues if they don't exist. Creates failed queues if they don't exist.
//
export async function getQrl (queue, qname, fqueue, fqname, options) {
  debug('getQrl', queue, qname, fqueue, fqname)
  // Normal queue
  let qrl
  try {
    qrl = await qrlCacheGet(qname)
  } catch (err) {
    debug('getQrl error', err)
    // Anything other than queue doesn't exist gets re-thrown
    if (err.code !== 'AWS.SimpleQueueService.NonExistentQueue') throw err

    // Create our queue if it doesn't exist
    let fqrl
    try {
      // Grab fail queue
      fqrl = await qrlCacheGet(fqname)
    } catch (err) {
      debug('getQrl error', err)
      // Anything other than queue doesn't exist gets re-thrown
      if (err.code !== 'AWS.SimpleQueueService.NonExistentQueue') throw err

      // Create fail queue if it doesn't exist
      if (options.verbose) console.error(chalk.blue('Creating fail queue ') + fqueue)
      fqrl = await createFailQueue(fqueue, fqname, null, options)
    }
    // Need to grab fail queue's ARN to create our queue
    const data = await getQueueAttributes(fqrl)

    // Then we can finally create our queue
    if (options.verbose) console.error(chalk.blue('Creating queue ') + queue)
    qrl = await createQueue(queue, qname, data.Attributes.QueueArn, options)
  }
  return qrl
}

//
// Enqueue a single command
// Returns a promise for the SQS API response.
//
export async function enqueue (queue, command, options) {
  debug('enqueue(', { queue, command }, ')')

  queue = normalizeQueueName(queue, options)
  const qname = options.prefix + queue
  const fqueue = normalizeFailQueueName(queue, options)
  const fqname = options.prefix + fqueue
  debug({ qname, fqueue, fqname })

  // Now that we have the queue, send our message
  const qrl = await getQrl(queue, qname, fqueue, fqname, options)
  debug({ qrl })
  return sendMessage(qrl, command, options)
}

//
// Enqueue many commands formatted as an array of {queue: ..., command: ...} pairs.
// Returns a promise for the total number of messages enqueued.
//
export async function enqueueBatch (pairs, options) {
  debug('enqueueBatch(', pairs, ')')

  function unpackPair (pair) {
    const queue = normalizeQueueName(pair.queue, options)
    const command = pair.command
    const qname = options.prefix + queue
    const fqueue = normalizeFailQueueName(queue, options)
    const fqname = options.prefix + fqueue
    return { queue, qname, fqueue, fqname, command }
  }

  // Find unique pairs
  const uniquePairMap = {}
  const uniquePairs = []
  pairs.forEach(pair => {
    if (!uniquePairMap[pair.queue]) {
      uniquePairMap[pair.queue] = true
      uniquePairs.push(pair)
    }
  })
  debug({ uniquePairMap, uniquePairs })

  // Prefetch unique qrls in parallel (creating as needed)
  requestCount = 0
  const pairPromises = uniquePairs
    .map(unpackPair)
    .map(u => getQrl(u.queue, u.qname, u.fqueue, u.fqname, options))
  await Promise.all(pairPromises)

  // After we've prefetched, all qrls are in cache
  // so go back through the list of pairs and fire off messages
  const addMessagePromises = []
  for (const { queue, qname, fqueue, fqname, command } of pairs.map(unpackPair)) {
    const qrl = await getQrl(queue, qname, fqueue, fqname, options)
    addMessagePromises.push(addMessage(qrl, command, options))
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
