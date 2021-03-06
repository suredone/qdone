
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
// Normalizes a queue name to end with .fifo if options.fifo is set
//
const fifoSuffix = '.fifo'
exports.fifoSuffix = fifoSuffix
exports.normalizeQueueName = function normalizeQueueName (qname, options) {
  const sliced = qname.slice(0, -fifoSuffix.length)
  const suffix = qname.slice(-fifoSuffix.length)
  const base = suffix === fifoSuffix ? sliced : qname
  return base + (options.fifo && qname.slice(-1) !== '*' ? fifoSuffix : '')
}

//
// Normalizes fail queue name, appending both --fail-suffix and .fifo depending on options
//
exports.normalizeFailQueueName = function normalizeFailQueueName (qname, options) {
  qname = exports.normalizeQueueName(qname, { fifo: false }) // strip .fifo if it is there
  const sliced = qname.slice(0, -options['fail-suffix'].length)
  const suffix = qname.slice(-options['fail-suffix'].length)
  const base = suffix === options['fail-suffix'] ? sliced : qname // strip --fail-suffix if it is there
  return (base + options['fail-suffix']) + (options.fifo ? fifoSuffix : '')
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
  if (Object.prototype.hasOwnProperty.call(qcache, qname)) return Q.fcall(_get, qname)
  return sqs
    .getQueueUrl({ QueueName: qname })
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
    const qname = path.basename((new url.URL(qrl)).pathname)
    qcache[qname] = qrl
    pairs.push({ qname: qname, qrl: qrl })
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
    return qname.slice(-1) === '*' // wildcard queues support
      ? sqs
        .listQueues({ QueueNamePrefix: qname.slice(0, -1) })
        .promise()
        .then(function (data) {
          debug('listQueues return', data)
          if (options.fifo) {
            // Remove non-fifo queues
            data.QueueUrls = (data.QueueUrls || []).filter(url => url.slice(-fifoSuffix.length) === fifoSuffix)
          }
          return ingestQRLs(data.QueueUrls || [])
        })
      : exports
        .get(qname)
        .then(function (qrl) {
          debug('qrlCache.get returned', qrl)
          return { qname: qname, qrl: qrl }
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
