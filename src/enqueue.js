
const Q = require('q')
const debug = require('debug')('qdone:enqueue')
const chalk = require('chalk')
const qrlCache = require('./qrlCache')
const AWS = require('aws-sdk')

function createDeadLetterQueue (fqueue, fqname) {
  debug('createDeadLetterQueue(', fqueue, fqname, ')')
  const sqs = new AWS.SQS()
  return sqs
    .createQueue({
      QueueName: fqname
    })
    .promise()
    .then(function (data) {
      debug('createDeadLetterQueue returned', data)
      return data.QueueUrl
    })
}

function createFailQueue (fqueue, fqname, deadLetterTargetArn) {
  debug('createFailQueue(', fqueue, fqname, ')')
  const sqs = new AWS.SQS()
  const params = {
    Attributes: { /* MessageRetentionPeriod: '259200', */ },
    QueueName: qname
  }
  if (deadLetterTargetArn) {
    params.Attributes['RedrivePolicy'] = `{"deadLetterTargetArn": "${deadLetterTargetArn}", "maxReceiveCount": "1"}'}`
  }
  return sqs
    .createQueue(params)
    .promise()
    .then(function (data) {
      debug('createFailQueue returned', data)
      return data.QueueUrl
    })
}

function createQueue (queue, qname, deadLetterTargetArn) {
  debug('createQueue(', queue, qname, ')')
  const sqs = new AWS.SQS()
  return sqs
    .createQueue({
      Attributes: {
        // MessageRetentionPeriod: '259200',
        RedrivePolicy: `{"deadLetterTargetArn": "${deadLetterTargetArn}", "maxReceiveCount": "1"}'}`
      },
      QueueName: qname
    })
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

function sendMessage (qrl, command) {
  debug('sendMessage(', qrl, command, ')')
  const message = Object.assign({QueueUrl: qrl}, formatMessage(command))
  const sqs = new AWS.SQS()
  return sqs
    .sendMessage(message)
    .promise()
    .then(function (data) {
      debug('sendMessage returned', data)
      return data
    })
}

function sendMessageBatch (qrl, messages) {
  debug('sendMessageBatch(', qrl, messages, ')')
  const params = { Entries: messages, QueueUrl: qrl }
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
  var batchMax = 10
  function whileNotEmpty () {
    if (!(messages[qrl] && messages[qrl].length)) return numFlushed
    // Construct batch until full
    const batch = []
    while (messages[qrl].length && batch.length < 10) {
      batch.push(messages[qrl].shift())
    }
    return sendMessageBatch(qrl, batch)
      .catch(function (err) {
        // Only handle request too long errors
        if (err.code !== 'AWS.SimpleQueueService.BatchRequestTooLong') throw err
        // And bail if one message is still too long
        if (batchMax === 1) throw err
        // Respond by cutting batch size in half
        batchMax = Math.max(1, batchMax / 2)
        // And replace all our messages on the internal queue and try again
        while (batch.length) messages[qrl].unshift(batch.pop())
      })
      .then(function (data) {
        // Fail if there are any individual message failures
        if (data.Failed && data.Failed.length) {
          const err = new Error('One or more message failures: ' + JSON.stringify(data.Failed))
          err.Failed = data.Failed
          throw err
        }
        // If we actually managed to flush any of them, reset the max batch size
        if (batch.length) {
          requestCount += 1
          data.Successful.forEach(message => {
            if (options.verbose) console.error(chalk.blue('Enqueued job ') + message.MessageId + chalk.blue(' request ' + requestCount))
          })
          numFlushed += batch.length
          batchMax = 10
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
              return createFailQueue(fqueue, fqname)
            }
            throw err // throw unhandled errors
          })

        // Need to grab fail queue's ARN to create our queue
        return fqrl
          .then(getQueueAttributes)
          .then(data => {
            if (options.verbose) console.error(chalk.blue('Creating queue ') + queue)
            return createQueue(queue, qname, data.Attributes.QueueArn)
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

  const qname = options.prefix + queue
  const fqueue = queue + options['fail-suffix']
  const fqname = options.prefix + fqueue

  // Now that we have the queue, send our message
  return getQrl(queue, qname, fqueue, fqname, options)
    .then(qrl => sendMessage(qrl, command))
}

//
// Enqueue many commands formatted as an array of {queue: ..., command: ...} pairs.
// Returns a promise for the total number of messages enqueued.
//
exports.enqueueBatch = function enqueueBatch (pairs, options) {
  debug('enqueueBatch(', pairs, ')')

  function unpackPair (pair) {
    const queue = pair.queue
    const command = pair.command
    const qname = options.prefix + queue
    const fqueue = queue + options['fail-suffix']
    const fqname = options.prefix + fqueue
    return { queue, qname, fqueue, fqname, command }
  }

  // Prefetch unique qrls in parallel (creating as needed)
  const qrls = {}
  requestCount = 0
  return Q.all(
    pairs
      .filter(pair => qrls[pair.queue] ? false : (qrls[pair.queue] = true)) // filter duplicates
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
