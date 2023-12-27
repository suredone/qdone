/**
 * A cache for maintaining queue urls. Avoids having to call the GetQueueUrl
 * api too often.
 */

import { URL } from 'url'
import { basename } from 'path'
import { ListQueuesCommand, GetQueueUrlCommand, QueueDoesNotExist } from '@aws-sdk/client-sqs'
import { getSQSClient } from './sqs.js'
import Debug from 'debug'
const debug = Debug('qdone:qrlCache')

// The actual cache is shared across callers.
const qcache = new Map()

//
// Normalizes a queue name to end with .fifo if opt.fifo is set
//
export const fifoSuffix = '.fifo'
export function normalizeQueueName (qname, opt) {
  const hasFifo = qname.endsWith(fifoSuffix)
  const needsFifo = opt.fifo && qname.slice(-1) !== '*'
  const needsPrefix = !qname.startsWith(opt.prefix)
  const base = hasFifo ? qname.slice(0, -fifoSuffix.length) : qname
  return (needsPrefix ? opt.prefix : '') + base + (needsFifo ? fifoSuffix : '')
}

//
// Normalizes fail queue name, appending both --fail-suffix and .fifo depending on opt
//
export function normalizeFailQueueName (fqname, opt) {
  const newopt = Object.assign({}, opt, { fifo: false })
  const base = normalizeQueueName(fqname, newopt) // strip fifo
  debug({ fqname, base, opt })
  const needsFail = !base.endsWith(opt.failSuffix)
  return base + (needsFail ? opt.failSuffix : '') + (opt.fifo ? fifoSuffix : '')
}

//
// Normalizes dlq queue name, appending both --dlq-suffix and .fifo depending on opt
//
export function normalizeDLQName (dqname, opt) {
  const newopt = Object.assign({}, opt, { fifo: false })
  const base = normalizeQueueName(dqname, newopt) // strip fifo
  const needsDead = !base.endsWith(opt.dlqSuffix)
  return base + (needsDead ? opt.dlqSuffix : '') + (opt.fifo ? fifoSuffix : '')
}

//
// Clear cache
//
export function qrlCacheClear () {
  qcache.clear()
}

//
// Get QRL (Queue URL)
// Returns a promise for the queue name
//
export async function qrlCacheGet (qname) {
  debug('get', qname, qcache)
  if (qcache.has(qname)) return qcache.get(qname)
  const client = getSQSClient()
  const input = { QueueName: qname }
  const cmd = new GetQueueUrlCommand(input)
  // debug({ cmd })
  const result = await client.send(cmd)
  // debug('result', result)
  // if (!result) throw new Error(`No such queue ${qname}`)
  const { QueueUrl: qrl } = result
  // debug('getQueueUrl returned', data)
  qcache.set(qname, qrl)
  // debug('qcache', Object.keys(qcache), 'get', qname, ' => ', qcache[qname])
  return qrl
}

//
// Set QRL (Queue URL)
// Immediately updates the cache
//
export function qrlCacheSet (qname, qrl) {
  qcache.set(qname, qrl)
  // debug('qcache', Object.keys(qcache), 'set', qname, ' => ', qcache[qname])
}

//
// Invalidate cache for given qname
//
export function qrlCacheInvalidate (qname) {
  // debug('qcache', Object.keys(qcache), 'delete', qname, ' (was ', qcache[qname], ')')
  qcache.delete(qname)
}

//
// Ingets multiple QRLs
// Extracts queue names from an array of QRLs and immediately updates the cache.
//
export function ingestQRLs (qrls) {
  const pairs = []
  debug('ingestQRLs', qrls)
  for (const qrl of qrls) {
    const qname = basename((new URL(qrl)).pathname)
    qrlCacheSet(qname, qrl)
    pairs.push({ qname, qrl })
    // debug('qcache', Object.keys(qcache), 'ingestQRLs', qname, ' => ', qcache[qname])
  }
  return pairs
}

/**
 * Returns qrls for queues matching the given prefix and regex.
 */
export async function getMatchingQueues (prefix, nextToken) {
  debug('getMatchingQueues', prefix, nextToken)
  const input = { QueueNamePrefix: prefix, MaxResults: 1000 }
  if (nextToken) input.NextToken = nextToken
  const client = getSQSClient()
  const command = new ListQueuesCommand(input)
  const result = await client.send(command)
  debug({ result })
  const { QueueUrls: qrls, NextToken: keepGoing } = result ?? {}
  debug({ qrls, keepGoing })
  if (keepGoing) (qrls ?? []).push(...await getMatchingQueues(prefix, keepGoing))
  return qrls ?? []
}

//
// Resolves into a flattened aray of {qname: ..., qrl: ...} objects.
//
export async function getQnameUrlPairs (qnames, opt) {
  const promises = qnames.map(
    async function getQueueUrlOrUrls (qname) {
      if (qname.slice(-1) === '*') {
        // Wildcard queues support
        const prefix = qname.slice(0, -1)
        const queues = await getMatchingQueues(prefix)
        debug('listQueues return', queues)
        if (opt.fifo) {
          // Remove non-fifo queues
          const filteredQueues = queues.filter(url => url.slice(-fifoSuffix.length) === fifoSuffix)
          return ingestQRLs(filteredQueues)
        } else {
          return ingestQRLs(queues)
        }
      } else {
        // Normal, non wildcard queue support
        try {
          return { qname, qrl: await qrlCacheGet(qname) }
        } catch (err) {
          if (err instanceof QueueDoesNotExist) return
        }
      }
    }
  )

  // Flatten nested results
  const results = await Promise.all(promises)
  return ([].concat.apply([], results)).filter(r => r)
}

debug('loaded')
