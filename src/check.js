import chalk from 'chalk'
import Debug from 'debug'
import {
  GetQueueAttributesCommand,
  SetQueueAttributesCommand,
  QueueDoesNotExist,
  RequestThrottled,
  KmsThrottled
} from '@aws-sdk/client-sqs'

import {
  qrlCacheGet,
  normalizeQueueName,
  normalizeFailQueueName,
  normalizeDLQName,
  getQnameUrlPairs
} from './qrlCache.js'
import { getSQSClient } from './sqs.js'
import {
  getDLQParams,
  getFailParams,
  getQueueParams,
  getOrCreateQueue,
  getOrCreateFailQueue,
  getOrCreateDLQ
} from './enqueue.js'
import { getOptionsWithDefaults } from './defaults.js'
import { ExponentialBackoff } from './exponentialBackoff.js'

const debug = Debug('qdone:check')

/**
 * Loops through attributes, checking each and returning true if they match
 */
export function attributesMatch (current, desired, opt, indent = '') {
  let match = true
  for (const attribute in desired) {
    if (current[attribute] !== desired[attribute]) {
      if (opt.verbose) console.error(chalk.yellow(indent + 'Attribute mismatch: ') + attribute + chalk.yellow(' should be ') + desired[attribute] + chalk.yellow(' but is ') + current[attribute])
      match = false
    }
  }
  return match
}

/**
 * Checks a DLQ, creating if the create option is set and modifying it if the
 * overwrite option is set.
 */
export async function checkDLQ (queue, qrl, opt, indent = '') {
  debug({ checkDLQ: { queue, qrl } })
  const dqname = normalizeDLQName(queue, opt)
  if (opt.verbose) console.error(chalk.blue(indent + 'checking ') + dqname)

  // Check DLQ
  let dqrl
  try {
    dqrl = await qrlCacheGet(dqname)
  } catch (e) {
    if (!(e instanceof QueueDoesNotExist)) throw e
    if (opt.verbose) console.error(chalk.red(indent + '  does not exist'))
    if (opt.create) {
      if (opt.verbose) console.error(chalk.green(indent + '  creating'))
      dqrl = await getOrCreateDLQ(queue, opt)
    } else {
      return
    }
  }

  // Check attributes
  const { params: { Attributes: desired } } = getDLQParams(queue, opt)
  const { Attributes: current } = await getQueueAttributes(dqrl)
  if (attributesMatch(current, desired, opt, indent + '  ')) {
    if (opt.verbose) console.error(chalk.green(indent + '  all good'))
  } else {
    if (opt.overwrite) {
      if (opt.verbose) console.error(chalk.green(indent + '  modifying'))
      return setQueueAttributes(dqrl, desired)
    }
  }
}

/**
 * Checks a fail queue, creating if the create option is set and modifying it if the
 * overwrite option is set.
 */
export async function checkFailQueue (queue, qrl, opt, indent = '') {
  // Check dead first
  await checkDLQ(queue, qrl, opt, indent)

  // Check fail queue
  const fqname = normalizeFailQueueName(queue, opt)
  let fqrl
  try {
    fqrl = await qrlCacheGet(fqname)
  } catch (e) {
    if (!(e instanceof QueueDoesNotExist)) throw e
    if (opt.verbose) console.error(chalk.red(indent + '  does not exist'))
    if (opt.create) {
      if (opt.verbose) console.error(chalk.green(indent + '  creating'))
      fqrl = await getOrCreateFailQueue(queue, opt)
    } else {
      return
    }
  }

  try {
    // Get fail queue params, creating fail queue if it doesn't exist and create flag is set
    if (opt.verbose) console.error(chalk.blue(indent + 'checking ') + fqname)
    const { params: { Attributes: desired } } = await getFailParams(queue, opt)
    const { Attributes: current } = await getQueueAttributes(fqrl)
    if (attributesMatch(current, desired, opt, indent + '  ')) {
      if (opt.verbose) console.error(chalk.green(indent + '  all good'))
    } else {
      if (opt.overwrite) {
        if (opt.verbose) console.error(chalk.green(indent + '  modifying'))
        return setQueueAttributes(fqrl, desired)
      }
    }
  } catch (e) {
    if (!(e instanceof QueueDoesNotExist)) throw e
    if (opt.verbose) console.error(chalk.red(indent + '  missing dlq'))
  }
}

/**
 * Checks a queue, creating or modifying it if the create option is set
 * and it needs it.
 */
export async function checkQueue (queue, qrl, opt, indent = '') {
  const qname = normalizeQueueName(queue, opt)
  if (opt.verbose) console.error(chalk.blue(indent + 'checking ') + qname)
  await checkFailQueue(queue, qrl, opt, indent + '  ')
  try {
    const { params: { Attributes: desired } } = await getQueueParams(queue, opt)
    const { Attributes: current, $metadata } = await getQueueAttributes(qrl)
    debug({ current, $metadata })
    if (attributesMatch(current, desired, opt, indent + '  ')) {
      if (opt.verbose) console.error(chalk.green(indent + '  all good'))
    } else {
      if (opt.overwrite) {
        if (opt.verbose) console.error(chalk.green(indent + '  modifying'))
        return setQueueAttributes(qrl, desired)
      }
    }
  } catch (e) {
    if (!(e instanceof QueueDoesNotExist)) throw e
    if (opt.verbose) console.error(chalk.red(indent + '  missing fail queue'))
  }
}

export async function getQueueAttributes (qrl) {
  debug('getQueueAttributes(', qrl, ')')
  const client = getSQSClient()
  const params = { AttributeNames: ['All'], QueueUrl: qrl }
  const cmd = new GetQueueAttributesCommand(params)
  // debug({ cmd })
  const data = await client.send(cmd)
  debug('GetQueueAttributes returned', data)
  return data
}

export async function setQueueAttributes (qrl, attributes) {
  debug('setQueueAttributes(', qrl, attributes, ')')
  const client = getSQSClient()
  const params = { Attributes: attributes, QueueUrl: qrl }
  const cmd = new SetQueueAttributesCommand(params)
  debug({ cmd })
  const data = await client.send(cmd)
  debug('SetQueueAttributes returned', data)
  return data
}

// Retry happens within the context of the send functions
const retryableExceptions = [
  RequestThrottled,
  KmsThrottled,
  QueueDoesNotExist // Queue could temporarily not exist due to eventual consistency, let it retry
]

//
// Enqueue a single command
// Returns a promise for the SQS API response.
//
export async function check (queues, options) {
  debug('check(', { queues }, ')')
  const opt = getOptionsWithDefaults(options)

  // Start processing
  if (opt.verbose) console.error(chalk.blue('Resolving queues: ') + queues.join(' '))
  const qnames = queues.map(queue => normalizeQueueName(queue, opt))
  const pairs = await getQnameUrlPairs(qnames, opt)

  // Figure out which queues we want to listen on, choosing between active and
  // all, filtering out failed queues if the user wants that
  const selectedPairs = pairs
    .filter(({ qname }) => qname)
    .filter(({ qname }) => {
      const suf = opt.failSuffix + (opt.fifo ? '.fifo' : '')
      const deadSuf = opt.dlqSuffix + (opt.fifo ? '.fifo' : '')
      const isFailQueue = qname.slice(-suf.length) === suf
      const isDeadQueue = qname.slice(-deadSuf.length) === deadSuf
      const isPlain = !isFailQueue && !isDeadQueue
      const shouldInclude = isPlain || (isFailQueue && opt.includeFailed) || (isDeadQueue && opt.includeDead)
      return shouldInclude
    })

  for (const { qname, qrl } of selectedPairs) {
    debug({ qname, qrl })
    await checkQueue(qname, qrl, opt)
  }

  debug({ pairs })
}

debug('loaded')