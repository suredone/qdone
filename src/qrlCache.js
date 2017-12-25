
const Q = require('q')
const debug = require('debug')('qdone:qrlCache')
const chalk = require('chalk')
const url = require('url')
const path = require('path')
const AWS = require('aws-sdk')

const qcache = {}

function _get (qname) {
  return qcache[qname]
}

//
// Clear cache
//
exports.clear = function clear () {
  Object.keys(qcache).forEach(function (key) {
    delete qcache[key]
  })
}

//
// Get QRL (Queue URL)
// Returns a promise for the queue name
//
exports.get = function get (qname) {
  debug('get', qname, qcache)
  const sqs = new AWS.SQS()
  if (qcache.hasOwnProperty(qname)) return Q.fcall(_get, qname)
  return sqs
    .getQueueUrl({QueueName: qname})
    .promise()
    .then(function (data) {
      debug('getQueueUrl returned', data)
      qcache[qname] = data.QueueUrl
      debug('qcache', Object.keys(qcache), 'get', qname, ' => ', qcache[qname])
      return Q.fcall(_get, qname)
    })
}

//
// Set QRL (Queue URL)
// Immediately updates the cache
//
exports.set = function set (qname, qrl) {
  qcache[qname] = qrl
  debug('qcache', Object.keys(qcache), 'set', qname, ' => ', qcache[qname])
}

//
// Invalidate cache for given qname
//
exports.invalidate = function invalidate (qname) {
  debug('qcache', Object.keys(qcache), 'delete', qname, ' (was ', qcache[qname], ')')
  delete qcache[qname]
}

//
// Ingets multiple QRLs
// Extracts queue names from an array of QRLs and immediately updates the cache.
//
function ingestQRLs (qrls) {
  const pairs = []
  debug('ingestQRLs', qrls)
  qrls.forEach(function (qrl) {
    const qname = path.basename(url.parse(qrl).pathname)
    qcache[qname] = qrl
    pairs.push({qname: qname, qrl: qrl})
    debug('qcache', Object.keys(qcache), 'ingestQRLs', qname, ' => ', qcache[qname])
  })
  return pairs
}

//
// Resolves into a flattened aray of {qname: ..., qrl: ...} objects.
//
exports.getQnameUrlPairs = function getQnameUrlPairs (qnames, options) {
  const sqs = new AWS.SQS()
  return Q.all(qnames.map(function (qname) {
    return qname.slice(-1) === '*'  // wildcard queues support
      ? sqs
          .listQueues({QueueNamePrefix: qname.slice(0, -1)})
          .promise()
          .then(function (data) {
            debug('listQueues return', data)
            return ingestQRLs(data.QueueUrls || [])
          })
      : exports
        .get(qname)
        .then(function (qrl) {
          debug('qrlCache.get returned', qrl)
          return {qname: qname, qrl: qrl}
        })
        .catch(function (err) {
          debug('qrlCache.get failed', err)
          if (options.verbose) console.error('  ' + chalk.red(qname.slice(options.prefix.length) + ' - ' + err))
        })
  }))
  .then(function (results) {
    // flatten nested results
    return ([].concat.apply([], results)).filter(r => r)
  })
}

debug('loaded')
