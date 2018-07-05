
const Q = require('q')
const debug = require('debug')('qdone:enqueue')
const chalk = require('chalk')
const uuid = require('uuid')
const qrlCache = require('./qrlCache')
const AWS = require('aws-sdk')

function createFailQueue (fqueue, fqname, deadLetterTargetArn, options) {
  debug('createFailQueue(', fqueue, fqname, ')')
  const sqs = new AWS.SQS()
  const params = {
    /* Attributes: {MessageRetentionPeriod: '259200', RedrivePolicy: '{"deadLetterTargetArn": "arn:aws:sqs:us-east-1:80398EXAMPLE:MyDeadLetterQueue", "maxReceiveCount": "1000"}'}, */
    Attributes: {},
    QueueName: fqname
  }
  if (options.fifo) params.Attributes.FifoQueue = 'true'
  return sqs
    .createQueue(params)
    .promise()
    .then(function (data) {
      debug('createQueue returned', data)
      return data.QueueUrl
    })
}

function createQueue (queue, qname, deadLetterTargetArn, options) {
  debug('createQueue(', queue, qname, ')')
  const sqs = new AWS.SQS()
  const params = {
    Attributes: {
      // MessageRetentionPeriod: '259200',
      RedrivePolicy: `{"deadLetterTargetArn": "${deadLetterTargetArn}", "maxReceiveCount": "1"}'}`
    },
    QueueName: qname
  }
  if (options.fifo) params.Attributes.FifoQueue = 'true'
  return sqs
    .createQueue(params)
    .promise()
    .then(function (data) {
      debug('createQueue returned', data)
      return data.QueueUrl
    })
}

function getQueueAttributes (qrl) {
  debug('getQueueAttributes(', qrl, ')')
  const sqs = new AWS.SQS()
  return sqs
    .getQueueAttributes({
      AttributeNames: ['All'],
      QueueUrl: qrl
    })
    .promise()
    .then(function (data) {
      debug('getQueueAttributes returned', data)
      return data
    })
}

function formatMessage (command, id) {
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

function sendMessage (qrl, command, options) {
  debug('sendMessage(', qrl, command, ')')
  const message = Object.assign({QueueUrl: qrl}, formatMessage(command))
  // Add in group id if we're using fifo
  if (options.fifo) {
    message.MessageGroupId = options['group-id']
    message.MessageDeduplicationId = uuid.v1()
  }
  const sqs = new AWS.SQS()
  return sqs
    .sendMessage(message)
    .promise()
    .then(function (data) {
      debug('sendMessage returned', data)
      return data
    })
}

function sendMessageBatch (qrl, messages, options) {
  debug('sendMessageBatch(', qrl, messages.map(e => Object.assign(Object.assign({}, e), {MessageBody: e.MessageBody.slice(0, 10) + '...'})), ')')
  const params = { Entries: messages, QueueUrl: qrl }
  // Add in group id if we're using fifo
  if (options.fifo) {
    params.Entries = params.Entries.map(
      message => Object.assign({
        MessageGroupId: options['group-id-per-message'] ? uuid.v1() : options['group-id'],
        MessageDeduplicationId: uuid.v1()
      }, message)
    )
  }
  const sqs = new AWS.SQS()
  return sqs
    .sendMessageBatch(params)
    .promise()
    .then(function (data) {
      debug('sendMessageBatch returned', data)
      return data
    })
}

const messages = {}
var requestCount = 0

//
// Flushes the internal message buffer for qrl.
// If the message is too large, batch is retried with half the messages.
// Returns number of messages flushed.
//
function flushMessages (qrl, options) {
  debug('flushMessages', qrl)
  // Flush until empty
  var numFlushed = 0
  function whileNotEmpty () {
    if (!(messages[qrl] && messages[qrl].length)) return numFlushed
    // Construct batch until full
    const batch = []
    var nextSize = JSON.stringify(messages[qrl][0]).length
    var totalSize = 0
    while ((totalSize + nextSize) < 262144 && messages[qrl].length && batch.length < 10) {
      batch.push(messages[qrl].shift())
      totalSize += nextSize
      if (messages[qrl].length) nextSize = JSON.stringify(messages[qrl][0]).length
      else nextSize = 0
    }
    return sendMessageBatch(qrl, batch, options)
      .then(function (data) {
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
      })
      .then(whileNotEmpty)
  }
  return whileNotEmpty()
}

//
// Adds a message to the inernal message buffer for the given qrl.
// Automaticaly flushes if queue has >= 10 messages.
// Returns number of messages flushed.
//
var messageIndex = 0
function addMessage (qrl, command, options) {
  const message = formatMessage(command, messageIndex++)
  messages[qrl] = messages[qrl] || []
  messages[qrl].push(message)
  if (messages[qrl].length > 10) {
    return flushMessages(qrl, options)
  }
  return 0
}

//
// Fetches (or returns cached) the qrl
//
function getQrl (queue, qname, fqueue, fqname, options) {
  debug('getQrl', queue, qname, fqueue, fqname)
  // Normal queue
  const qrl = qrlCache
    .get(qname)
    .catch(function (err) {
      // Create our queue if it doesn't exist
      if (err.code === 'AWS.SimpleQueueService.NonExistentQueue') {
        // Grab fail queue
        const fqrl = qrlCache
          .get(fqname)
          .catch(function (err) {
            // Create fail queue if it doesn't exist
            if (err.code === 'AWS.SimpleQueueService.NonExistentQueue') {
              if (options.verbose) console.error(chalk.blue('Creating fail queue ') + fqueue)
              return createFailQueue(fqueue, fqname, null, options)
            }
            throw err // throw unhandled errors
          })

        // Need to grab fail queue's ARN to create our queue
        return fqrl
          .then(getQueueAttributes)
          .then(data => {
            if (options.verbose) console.error(chalk.blue('Creating queue ') + queue)
            return createQueue(queue, qname, data.Attributes.QueueArn, options)
          })
      }
      throw err // throw unhandled errors
    })

  return qrl
}

//
// Enqueue a single command
// Returns a promise for the SQS API response.
//
exports.enqueue = function enqueue (queue, command, options) {
  debug('enqueue(', queue, command, ')')

  queue = qrlCache.normalizeQueueName(queue, options)
  const qname = options.prefix + queue
  const fqueue = qrlCache.normalizeFailQueueName(queue, options)
  const fqname = options.prefix + fqueue

  // Now that we have the queue, send our message
  return getQrl(queue, qname, fqueue, fqname, options)
    .then(qrl => sendMessage(qrl, command, options))
}

//
// Enqueue many commands formatted as an array of {queue: ..., command: ...} pairs.
// Returns a promise for the total number of messages enqueued.
//
exports.enqueueBatch = function enqueueBatch (pairs, options) {
  debug('enqueueBatch(', pairs, ')')

  function unpackPair (pair) {
    const queue = qrlCache.normalizeQueueName(pair.queue, options)
    const command = pair.command
    const qname = options.prefix + queue
    const fqueue = qrlCache.normalizeFailQueueName(queue, options)
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
  debug({uniquePairMap, uniquePairs})

  // Prefetch unique qrls in parallel (creating as needed)
  requestCount = 0
  return Q.all(
    uniquePairs
      .map(unpackPair)
      .map(u => getQrl(u.queue, u.qname, u.fqueue, u.fqname, options))
  ).then(function () {
    // After we've prefetched, all qrls are in cache
    // so go back through the list of pairs and fire off messages
    return Q.all(
      // Add every individual command, flushing as we go
      pairs
        .map(unpackPair)
        .map(u =>
          getQrl(u.queue, u.qname, u.fqueue, u.fqname, options)
            .then(qrl => addMessage(qrl, u.command, options))
        )
    ).then(function (flushCounts) {
      // Count up how many were flushed during add
      debug('flushCounts', flushCounts)
      const totalFlushed = flushCounts.reduce((a, b) => a + b, 0)
      // And flush any remaining messages
      return Q
        .all(Object.keys(messages).map(key => flushMessages(key, options))) // messages is the global flush buffer
        .then(flushCounts => flushCounts.reduce((a, b) => a + b, totalFlushed))
    })
  })
}

debug('loaded')
