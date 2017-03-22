
// const Q = require('q')
const debug = require('debug')('qdone:enqueue')
const chalk = require('chalk')
const qrlCache = require('./qrlCache')
const SQS = require('aws-sdk/clients/sqs')
const sqs = new SQS()

exports.enqueue = function enqueue (queue, command, options, prefix) {
  debug('queue', queue, 'command', command)
  const qname = prefix + queue

  function constructAndSendMessage (qrl) {
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
  }

  debug('looking for queue named', qname)

  return qrlCache
    .get(qname)
    .then(constructAndSendMessage)
    .fail(function (err) {
      switch (err.code) {
        case 'AWS.SimpleQueueService.NonExistentQueue':
          // Create queue if it doesn't exist
          const params = {/* Attributes: {MessageRetentionPeriod: '259200', RedrivePolicy: '{"deadLetterTargetArn": "arn:aws:sqs:us-east-1:80398EXAMPLE:MyDeadLetterQueue", "maxReceiveCount": "1000"}'}, */
            QueueName: qname
          }
          console.error(chalk.blue('Creating queue ') + qname.slice(prefix.length))
          return sqs
            .createQueue(params, function (err, data) { if (err) throw err })
            .promise()
            .then(function (data) {
              debug('createQueue returned', data)
              console.error(chalk.blue('           --> ') + data.QueueUrl)
              qrlCache.set(qname, data.QueueUrl)
              return constructAndSendMessage(data.QueueUrl)
            })
            .fail(function (err) {
              throw err
            })
        default:
          throw err
      }
    })
}

debug('loaded')
