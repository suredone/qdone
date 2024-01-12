/**
 * Component to manange what queues are being listened to and in what order.
 */

import chalk from 'chalk'
import Debug from 'debug'

import { normalizeQueueName, getQnameUrlPairs } from '../qrlCache.js'
import { cheapIdleCheck } from '../idleQueues.js'
import { getSQSClient } from '../sqs.js'

const debug = Debug('qdone:queueManager')

export class QueueManager {
  constructor (opt, queues, resolveSeconds = 10) {
    this.opt = opt
    this.queues = queues
    this.resolveSeconds = resolveSeconds
    this.selectedPairs = []
    this.resolveTimeout = undefined
    this.shutdownRequested = false
    this.resolveQueues()
  }

  async resolveQueues () {
    if (this.shutdownRequested) return

    // Start processing
    if (this.opt.verbose) console.error(chalk.blue('Resolving queues: ') + this.queues.join(' '))
    const qnames = this.queues.map(queue => normalizeQueueName(queue, this.opt))
    const pairs = await getQnameUrlPairs(qnames, this.opt)

    if (this.shutdownRequested) return

    // Figure out which pairs are active
    const activePairs = []
    if (this.opt.activeOnly) {
      debug({ pairsBeforeCheck: pairs })
      await Promise.all(pairs.map(async pair => {
        const { idle } = await cheapIdleCheck(pair.qname, pair.qrl, this.opt)
        if (!idle) activePairs.push(pair)
      }))
    }

    if (this.shutdownRequested) return

    // Finished resolving
    if (this.opt.verbose) {
      console.error(chalk.blue('  done'))
      console.error()
    }

    // Figure out which queues we want to listen on, choosing between active and
    // all, filtering out failed queues if the user wants that
    this.selectedPairs = (this.opt.activeOnly ? activePairs : pairs)
      .filter(({ qname }) => {
        const suf = this.opt.failSuffix + (this.opt.fifo ? '.fifo' : '')
        const isFailQueue = qname.slice(-suf.length) === suf
        const shouldInclude = this.opt.includeFailed ? true : !isFailQueue
        return shouldInclude
      })

    // Randomize order
    this.selectedPairs.sort(() => 0.5 - Math.random())
    debug('selectedPairs', this.selectedPairs)

    if (this.opt.verbose) {
      console.error(chalk.blue('Will resolve queues again in ' + this.resolveSeconds + ' seconds'))
    }
    this.resolveTimeout = setTimeout(() => this.resolveQueues(), this.resolveSeconds * 1000)
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

  shutdown () {
    clearTimeout(this.resolveTimeout)
    this.shutdownRequested = true
  }
}
