/**
 * Component to manange what queues are being listened to and in what order.
 */

import chalk from 'chalk'
import Debug from 'debug'

import { normalizeQueueName, getQnameUrlPairs } from '../qrlCache.js'
import { cheapIdleCheck } from '../idleQueues.js'

const debug = Debug('qdone:queueManager')

export class QueueManager {
  constructor (opt, queues, resolveSeconds = 10) {
    this.opt = opt
    this.queues = queues
    this.resolveSeconds = resolveSeconds
    this.selectedPairs = []
    this.icehouse = {}
    this.resolveTimeout = undefined
    this.shutdownRequested = false
    this.resolvePromise = this.resolveQueues()
  }

  // Sends a queue to the icehouse, where it waits for a while before being
  // checked again
  updateIcehouse (qrl, emptyReceive) {
    const foundEntry = !!this.icehouse[qrl]
    const { lastCheck, secondsToWait, numEmptyReceives } = this.icehouse[qrl] || {
      lastCheck: new Date(),
      secondsToWait: Math.round(20 + 10 * Math.random()),
      numEmptyReceives: 0 + emptyReceive
    }
    if (emptyReceive) {
      const now = new Date()
      const secondsElapsed = lastCheck - now
      const minWait = 10
      const maxWait = 600
      const baseSeconds = numEmptyReceives ** 2 * 20
      const jitterSeconds = Math.round((Math.random() - 0.5) * baseSeconds)
      const newSecondsToWait = Math.max(minWait, Math.min(maxWait, baseSeconds + jitterSeconds))
      const newEntry = { lastCheck: now, secondsToWait: newSecondsToWait, numEmptyReceives: numEmptyReceives + 1 }
      this.icehouse[qrl] = newEntry
      if (this.opt.verbose) {
        console.error(chalk.blue('Sending queue to icehouse'), qrl, chalk.blue('for'), newSecondsToWait, chalk.blue('seconds'))
      }
      debug({ foundEntry, newEntry, lastCheck, secondsToWait, now, secondsElapsed, maxWait, minWait, baseSeconds, jitterSeconds })
    } else {
      delete this.icehouse[qrl]
    }
  }

  // Returns true if the queue should be kept in the icehouse
  keepInIcehouse (qrl, now) {
    if (this.icehouse[qrl]) {
      const { lastCheck, secondsToWait } = this.icehouse[qrl]
      const secondsElapsed = Math.round((now - lastCheck) / 1000)
      debug({ icehouseCheck: { qrl, lastCheck, secondsToWait, secondsElapsed } })
      const letOut = secondsElapsed > secondsToWait
      if (letOut && this.opt.verbose) {
        console.error(chalk.blue('Coming out of icehouse:'), qrl)
      }
      return !letOut
    } else {
      return false
    }
  }

  async resolveQueues () {
    clearTimeout(this.resolveTimeout)
    if (this.shutdownRequested) return

    // Start processing
    const qnames = this.queues.map(queue => normalizeQueueName(queue, this.opt))
    const pairs = await getQnameUrlPairs(qnames, this.opt)
    if (this.opt.verbose) console.error(chalk.blue('Resolving queues:'))

    if (this.shutdownRequested) return

    // Filter out queues
    const now = new Date()
    const filteredPairs = pairs
      // first failed
      .filter(({ qname, qrl }) => {
        const suf = this.opt.failSuffix + (this.opt.fifo ? '.fifo' : '')
        const isFailQueue = qname.slice(-suf.length) === suf
        return this.opt.includeFailed ? true : !isFailQueue
      })
      // first fifo
      .filter(({ qname, qrl }) => {
        const isFifo = qname.endsWith('.fifo')
        return this.opt.fifo ? isFifo : !isFifo
      })
      // then icehouse
      .filter(({ qname, qrl }) => !this.keepInIcehouse(qrl, now))

    // Figure out which pairs are active
    const activePairs = []
    if (this.opt.activeOnly) {
      if (this.opt.verbose) {
        console.error(chalk.blue('  checking active only'))
      }
      await Promise.all(filteredPairs.map(async ({ qname, qrl }) => {
        const { result: { idle } } = await cheapIdleCheck(qname, qrl, this.opt)
        debug({ idle, qname })
        if (!idle) activePairs.push({ qname, qrl })
      }))
    }

    if (this.shutdownRequested) return

    // Figure out which queues we want to listen on, choosing between active and
    // all, filtering out failed queues if the user wants that
    this.selectedPairs = (this.opt.activeOnly ? activePairs : filteredPairs)

    // Randomize order
    this.selectedPairs.sort(() => 0.5 - Math.random())
    if (this.opt.verbose) console.error(chalk.blue('  selected:\n  ') + this.selectedPairs.map(({ qname }) => qname).join('\n  '))
    debug('selectedPairs', this.selectedPairs)

    // Finished resolving
    if (this.opt.verbose) {
      console.error(chalk.blue('  done'))
      console.error()
    }

    if (this.opt.verbose) {
      console.error(chalk.blue('Will resolve queues again in ' + this.resolveSeconds + ' seconds'))
    }
    this.resolveTimeout = setTimeout(() => {
      this.resolvePromise = this.resolveQueues()
    }, this.resolveSeconds * 1000)
  }

  // Return the next queue in the lineup
  nextPair () {
    const pair = this.selectedPairs.shift()
    this.selectedPairs.push(pair)
    return pair
  }

  getPairs () {
    return this.selectedPairs
  }

  async shutdown () {
    this.shutdownRequested = true
    clearTimeout(this.resolveTimeout)
    await this.resolvePromise
  }
}
