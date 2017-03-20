
const debug = require('debug')('qdone:qrlCache')
const url = require('url')
const path = require('path')
const SQS = require('aws-sdk/clients/sqs')
const sqs = new SQS()

const qcache = {}

exports.get = function get (qname, callback) {
  if (qcache.hasOwnProperty(qname)) return callback(null, qcache[qname])
  sqs.getQueueUrl({QueueName: qname}, function (err, data) {
    debug('getQueueUrl returned', err, data)
    if (err) return callback(err)
    qcache[qname] = data.QueueUrl
    return callback(null, qcache[qname])
  })
  debug(qcache)
}

exports.set = function get (qname, qrl) {
  qcache[qname] = qrl
  debug(qcache)
}

exports.setFromQRLs = function get (qrls) {
  qrls.forEach(function (qrl) {
    const qname = path.basename(url.parse(qrl).pathname)
    qcache[qname] = qrl
  })
  debug(qcache)
}

debug('loaded')
