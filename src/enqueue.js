
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

function sendMessage (qrl, command) {
  debug('sendMessage(', qrl, command, ')')
  const message = {
    /*
    MessageAttributes: {
      City: { DataType: 'String', StringValue: 'Any City' },
      Population: { DataType: 'Number', StringValue: '1250800' }
    },
    */
    MessageBody: command,
    QueueUrl: qrl
  }
  debug('constructAndSendMessage', qrl, message)
  return sqs
    .sendMessage(message)
    .promise()
    .then(function (data) {
      debug('sendMessage returned', data)
      return data
    })
}

exports.enqueue = function enqueue (queue, command, options, globalOptions) {
  debug('enqueueue(', queue, command, ')')

  const qname = globalOptions.prefix + queue
  const fqueue = queue + globalOptions.failSuffix
  const fqname = globalOptions.prefix + fqueue

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

  // Now that we have the queue, send our message
  return qrl
    .then(function (qrl) {
      return sendMessage(qrl, command)
    })
}

debug('loaded')
