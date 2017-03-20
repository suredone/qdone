
const debug = require('debug')('qdone:worker')
const qrlCache = require('./qrlCache')
const SQS = require('aws-sdk/clients/sqs')
const sqs = new SQS()

exports.listen = function listen (queues, callback) {
  debug('queues', arguments)
  qrlCache
  sqs
}

debug('loaded')
