
const Q = require('q')
const debug = require('debug')('qdone:enqueue')
const chalk = require('chalk')
const qrlCache = require('./qrlCache')
const SQS = require('aws-sdk/clients/sqs')
const sqs = new SQS()

function createFailQueue (fqueue, fqname) {
  debug('createFailQueue(', fqueue, fqname, ')')
  console.error(chalk.blue('Creating fail queue ') + fqueue)
  return sqs
    .createQueue({/* Attributes: {MessageRetentionPeriod: '259200', RedrivePolicy: '{"deadLetterTargetArn": "arn:aws:sqs:us-east-1:80398EXAMPLE:MyDeadLetterQueue", "maxReceiveCount": "1000"}'}, */
      QueueName: fqname
    })
    .promise()
    .then(function (data) {
      debug('createQueue returned', data)
      return data.QueueUrl
    })
}

function createQueue (queue, qname, deadLetterTargetArn) {
  debug('createQueue(', queue, qname, ')')
  console.error(chalk.blue('Creating queue ') + queue)
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
  console.error(chalk.blue('Looking up attributes for ') + qrl)
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
function flushMessages (qrl) {
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
            console.error(chalk.blue('Enqueued job ') + message.MessageId + chalk.blue(' request ' + requestCount))
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
function addMessage (qrl, command, index) {
  const message = formatMessage(command, index)
  messages[qrl] = messages[qrl] || []
  messages[qrl].push(message)
  if (messages[qrl].length > 10) {
    return flushMessages(qrl)
  }
  return 0
}

//
// Fetches (or returns cached) the qrl
//
function getQrl (queue, qname, fqueue, fqname) {
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
              return createFailQueue(fqueue, fqname)
            }
            throw err // throw unhandled errors
          })

        // Need to grab fail queue's ARN to create our queue
        return fqrl
          .then(getQueueAttributes)
          .then(function (data) {
            return createQueue(queue, qname, data.Attributes.QueueArn)
          })
      }
      throw err // throw unhandled errors
    })

  return qrl
}

exports.enqueue = function enqueue (queue, command, options, globalOptions) {
  debug('enqueue(', queue, command, ')')

  const qname = globalOptions.prefix + queue
  const fqueue = queue + globalOptions.failSuffix
  const fqname = globalOptions.prefix + fqueue

  // Now that we have the queue, send our message
  return getQrl(queue, qname, fqueue, fqname)
    .then(function (qrl) {
      return sendMessage(qrl, command)
    })
}

exports.enqueueBatch = function enqueueBatch (pairs, options, globalOptions) {
  debug('enqueueBatch(', pairs, ')')
  return Q.all(
    // Add every individual command, flushing as we go
    pairs.map((pair, index) => {
      const queue = pair.queue
      const command = pair.command
      const qname = globalOptions.prefix + queue
      const fqueue = queue + globalOptions.failSuffix
      const fqname = globalOptions.prefix + fqueue
      // Now that we have the queue, send our message
      return getQrl(queue, qname, fqueue, fqname)
        .then(function (qrl) {
          return addMessage(qrl, command, index)
        })
    })
  ).then(function (numsFlushed) {
    debug('numsFlushed', numsFlushed)
    const numFlushed = numsFlushed.reduce((a, b) => a + b, 0)
    // Flush any remaining messages at the end
    return Q
      .all(Object.keys(messages).map(qrl => flushMessages(qrl)))
      .then(numsFlushed => numsFlushed.reduce((a, b) => a + b, numFlushed))
  })
}

debug('loaded')
