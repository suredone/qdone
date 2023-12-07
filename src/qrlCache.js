/**
 * A cache for maintaining queue urls. Avoids having to call the GetQueueUrl
 * api too often.
 */

import { URL } from 'url'
import { basename } from 'path'
import { ListQueuesCommand, GetQueueUrlCommand } from '@aws-sdk/client-sqs'
import { getClient } from './sqs.js'
import Debug from 'debug'
const debug = Debug('qdone:qrlCache')

// The actual cache is shared across callers.
const qcache = new Map()

//
// Normalizes a queue name to end with .fifo if options.fifo is set
//
export const fifoSuffix = '.fifo'
export function normalizeQueueName (qname, options) {
  const sliced = qname.slice(0, -fifoSuffix.length)
  const suffix = qname.slice(-fifoSuffix.length)
  const base = suffix === fifoSuffix ? sliced : qname
  return base + (options.fifo && qname.slice(-1) !== '*' ? fifoSuffix : '')
}

//
// Normalizes fail queue name, appending both --fail-suffix and .fifo depending on options
//
export function normalizeFailQueueName (qname, options) {
  qname = normalizeQueueName(qname, { fifo: false }) // strip .fifo if it is there
  const sliced = qname.slice(0, -(options['fail-suffix']?.length || 0))
  const suffix = qname.slice(-(options['fail-suffix']?.length || 0))
  const base = suffix === options['fail-suffix'] ? sliced : qname // strip --fail-suffix if it is there
  return (base + options['fail-suffix']) + (options.fifo ? fifoSuffix : '')
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
  const client = getClient()
  const input = { QueueName: qname }
  const cmd = new GetQueueUrlCommand(input)
  debug({ cmd })
  const result = await client.send(cmd)
  debug({ result })
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
export async function getMatchingQueues (prefix) {
  const input = { QueueNamePrefix: prefix, MaxResults: 1000 }
  const client = getClient()
  async function processQueues (nextToken) {
    if (nextToken) input.NextToken = nextToken
    const command = new ListQueuesCommand(input)
    // debug({ nextToken, input, command })
    const result = await client.send(command)
    debug({ result })
    const { QueueUrls: qrls, NextToken: nextToken2 } = result ?? { QueueUrls: [] }
    // debug({ qrls, nextToken2 })
    return qrls.concat(nextToken2 ? await processQueues(nextToken2) : [])
  }
  return processQueues()
}

//
// Resolves into a flattened aray of {qname: ..., qrl: ...} objects.
//
export async function getQnameUrlPairs (qnames, options) {
  const promises = qnames.map(
    async function getQueueUrlOrUrls (qname) {
      if (qname.slice(-1) === '*') {
        // Wildcard queues support
        const prefix = qname.slice(0, -1)
        const queues = await getMatchingQueues(prefix)
        debug('listQueues return', queues)
        if (options.fifo) {
          // Remove non-fifo queues
          const filteredQueues = queues.filter(url => url.slice(-fifoSuffix.length) === fifoSuffix)
          return ingestQRLs(filteredQueues)
        } else {
          return ingestQRLs(queues)
        }
      } else {
        // Normal, non wildcard queue support
        return { qname, qrl: await qrlCacheGet(qname) }
      }
    }
  )

  // Flatten nested results
  const results = await Promise.all(promises)
  return ([].concat.apply([], results)).filter(r => r)
}

debug('loaded')
