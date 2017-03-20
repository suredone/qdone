
const debug = require('debug')('qdone:enqueue')
const qrlCache = require('./qrlCache')
const SQS = require('aws-sdk/clients/sqs')
const sqs = new SQS()

exports.enqueue = function enqueue (queue, command, options, callback) {
  debug('queue', queue, 'command', command)
  const qname = options.prefix + queue

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
    sqs.sendMessage(message, function (err, data) {
      debug('sendMessage returned', err, data)
      if (err) return callback(err)
      callback(null, data)
    })
  }

  debug('looking for queue named', qname)
  qrlCache.get(qname, function (err, qrl) {
    if (err) {
      switch (err.code) {
        case 'AWS.SimpleQueueService.NonExistentQueue':
          // Create queue if it doesn't exist
          const params = {/* Attributes: {MessageRetentionPeriod: '259200', RedrivePolicy: '{"deadLetterTargetArn": "arn:aws:sqs:us-east-1:80398EXAMPLE:MyDeadLetterQueue", "maxReceiveCount": "1000"}'}, */
            QueueName: qname
          }
          debug('creating queue', params)
          sqs.createQueue(params, function (err, data) {
            debug('createQueue returned', err, data)
            if (err) return callback(err)
            qrlCache.set(qname, data.QueueUrl)
            constructAndSendMessage(data.QueueUrl)
          })
          break

        default:
          return callback(err)
      }
    } else {
      constructAndSendMessage(qrl)
    }
  })
}

debug('loaded')
