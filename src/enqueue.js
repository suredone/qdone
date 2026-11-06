import { setExtra } from '@sentry/node'
import { v1 as uuidV1 } from 'uuid'
import chalk from 'chalk'
import Debug from 'debug'
import {
  CreateQueueCommand,
  GetQueueAttributesCommand,
  SendMessageCommand,
  SendMessageBatchCommand,
  QueueDoesNotExist,
  RequestThrottled,
  KmsThrottled
} from '@aws-sdk/client-sqs'

import {
  qrlCacheGet,
  qrlCacheSet,
  qrlCacheInvalidate,
  normalizeQueueName,
  normalizeFailQueueName,
  normalizeDLQName
} from './qrlCache.js'
import { getSQSClient } from './sqs.js'
import {
  addDedupParamsToMessage,
  claimDedup,
  confirmDedup,
  messageDedupId,
  releasePendingDedup
} from './dedup.js'
import { getOptionsWithDefaults, validateMessageOptions } from './defaults.js'
import { ExponentialBackoff } from './exponentialBackoff.js'
import { redactCacheUri } from './sentry.js'

const debug = Debug('qdone:enqueue')

export function getDLQParams (queue, opt) {
  const dqname = normalizeDLQName(queue, opt)
  const params = {
    Attributes: { MessageRetentionPeriod: opt.messageRetentionPeriod + '' },
    QueueName: dqname
  }
  if (opt.tags) params.tags = opt.tags
  if (opt.fifo) params.Attributes.FifoQueue = 'true'
  return { dqname, params }
}

export async function getOrCreateDLQ (queue, opt) {
  debug('getOrCreateDLQ(', queue, ')')
  const { dqname, params } = getDLQParams(queue, opt)
  try {
    const dqrl = await qrlCacheGet(dqname)
    return dqrl
  } catch (err) {
    // Anything other than queue doesn't exist gets re-thrown
    if (!(err instanceof QueueDoesNotExist)) throw err

    // Create our DLQ
    const client = getSQSClient()
    const cmd = new CreateQueueCommand(params)
    if (opt.verbose) console.error(chalk.blue('Creating dead letter queue ') + dqname)
    const data = await client.send(cmd)
    debug('createQueue returned', data)
    const dqrl = data.QueueUrl
    qrlCacheSet(dqname, dqrl)
    return dqrl
  }
}

/**
 * Returns the parameters needed for creating a failed queue. If DLQ options
 * are set, it makes an API call to get this DLQ's ARN.
 */
export async function getFailParams (queue, opt) {
  const fqname = normalizeFailQueueName(queue, opt)
  const params = {
    Attributes: { MessageRetentionPeriod: opt.messageRetentionPeriod + '' },
    QueueName: fqname
  }
  // If we have a dlq, we grab it and set a redrive policy
  if (opt.dlq) {
    const dqname = normalizeDLQName(queue, opt)
    const dqrl = await qrlCacheGet(dqname)
    const dqa = await getQueueAttributes(dqrl)
    debug('dqa', dqa)
    params.Attributes.RedrivePolicy = JSON.stringify({
      deadLetterTargetArn: dqa.Attributes.QueueArn,
      maxReceiveCount: opt.dlqAfter
    })
  }
  if (opt.failDelay) params.Attributes.DelaySeconds = opt.failDelay + ''
  if (opt.tags) params.tags = opt.tags
  if (opt.fifo) params.Attributes.FifoQueue = 'true'
  return params
}

/**
 * Returns the qrl for the failed queue for the given queue. Creates the queue
 * if it does not exist.
 */
export async function getOrCreateFailQueue (queue, opt, doesNotExist) {
  debug('getOrCreateFailQueue(', queue, ')')
  const fqname = normalizeFailQueueName(queue, opt)
  try {
    // Bail early if the caller knew we didn't have a queue
    if (doesNotExist) throw new QueueDoesNotExist(fqname)
    const fqrl = await qrlCacheGet(fqname)
    return fqrl
  } catch (err) {
    // Anything other than queue doesn't exist gets re-thrown
    if (!(err instanceof QueueDoesNotExist)) throw err

    // Grab params, creating DLQ if needed
    let params
    try {
      params = await getFailParams(queue, opt)
    } catch (e) {
      // If DLQ doesn't exist, create it
      if (!(opt.dlq && e instanceof QueueDoesNotExist)) throw e
      await getOrCreateDLQ(queue, opt)
      params = await getFailParams(queue, opt)
    }

    // Create our fail queue
    const client = getSQSClient()
    const cmd = new CreateQueueCommand(params)
    if (opt.verbose) console.error(chalk.blue('Creating fail queue ') + fqname)
    const data = await client.send(cmd)
    debug('createQueue returned', data)
    const fqrl = data.QueueUrl
    qrlCacheSet(fqname, fqrl)
    return fqrl
  }
}

/**
 * Returns the parameters needed for creating a queue. If fail options
 * are set, it makes an API call to get the fail queue's ARN.
 */
export async function getQueueParams (queue, opt) {
  const qname = normalizeQueueName(queue, opt)
  const fqname = normalizeFailQueueName(queue, opt)
  const fqrl = await qrlCacheGet(fqname, opt)
  const fqa = await getQueueAttributes(fqrl)
  const params = {
    Attributes: {
      MessageRetentionPeriod: opt.messageRetentionPeriod + '',
      RedrivePolicy: JSON.stringify({
        deadLetterTargetArn: fqa.Attributes.QueueArn,
        maxReceiveCount: 1
      })
    },
    QueueName: qname
  }
  if (opt.tags) params.tags = opt.tags
  if (opt.fifo) params.Attributes.FifoQueue = 'true'
  return params
}

/**
 * Returns a qrl for a queue that either exists or does not
 */
export async function getOrCreateQueue (queue, opt) {
  debug('getOrCreateQueue(', queue, ')')
  const qname = normalizeQueueName(queue, opt)
  try {
    const qrl = await qrlCacheGet(qname)
    return qrl
  } catch (err) {
    // Anything other than queue doesn't exist gets re-thrown
    if (!(err instanceof QueueDoesNotExist)) throw err

    // Grab params, creating fail queue if needed
    let params
    try {
      params = await getQueueParams(qname, opt)
    } catch (e) {
      // If fail queue doesn't exist, create it
      if (!(e instanceof QueueDoesNotExist)) throw e
      await getOrCreateFailQueue(qname, opt, true)
      params = await getQueueParams(qname, opt)
    }

    debug({ getOrCreateQueue: { qname, params } })

    // Create our queue
    const client = getSQSClient()
    const cmd = new CreateQueueCommand(params)
    if (opt.verbose) console.error(chalk.blue('Creating fail queue ') + qname)
    const data = await client.send(cmd)
    debug('AWS createQueue returned', data)
    const qrl = data.QueueUrl
    qrlCacheSet(qname, qrl)
    return qrl
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

export function formatMessage (body, id, opt, messageOptions) {
  const message = { MessageBody: body }
  if (typeof id !== 'undefined') message.Id = '' + id
  if (opt.fifo) {
    message.MessageGroupId = messageOptions?.groupId || opt?.groupId
  }
  addDedupParamsToMessage(message, opt, messageOptions)
  if (opt.delay) message.DelaySeconds = opt.delay
  if (messageOptions?.delay) message.DelaySeconds = messageOptions.delay
  return message
}

// Retry happens within the context of the send functions
const retryableExceptions = [
  RequestThrottled,
  KmsThrottled,
  QueueDoesNotExist // Queue could temporarily not exist due to eventual consistency, let it retry
]

function definitiveRequestError (error) {
  return retryableExceptions.some(Exception => error instanceof Exception) &&
    error.$metadata?.attempts === 1 &&
    Number.isInteger(error.$metadata.httpStatusCode) &&
    error.$metadata.httpStatusCode >= 400 && error.$metadata.httpStatusCode < 500 &&
    error.$metadata.httpStatusCode !== 408
}

async function sendPrepared (queue, params, opt, Command, custody) {
  const client = getSQSClient()
  let cmd = new Command(params)
  const backoff = new ExponentialBackoff(opt.sendRetries)
  const send = async attemptNumber => {
    if (Command === SendMessageCommand) cmd.input.attemptNumber = attemptNumber
    custody.started = true
    try {
      return await client.send(cmd)
    } catch (error) {
      if (!definitiveRequestError(error)) custody.uncertain = true
      throw error
    }
  }
  const shouldRetry = async (result, error) => {
    if (error instanceof QueueDoesNotExist) {
      qrlCacheInvalidate(normalizeQueueName(queue, opt))
      params.QueueUrl = await getOrCreateQueue(queue, opt)
      cmd = new Command(params)
    }
    return retryableExceptions.some(Exception => error instanceof Exception)
  }
  return backoff.run(send, shouldRetry)
}

function hasMessageId (value) {
  return typeof value === 'string' && value.trim().length > 0
}

function knownAcceptance (outcome) {
  return outcome.status === 'accepted' || outcome.status === 'deduplicated'
}

export class EnqueueAcceptanceError extends Error {
  constructor (outcomes, results, protocolFailure = false) {
    super('One or more message failures or unknown enqueue outcomes')
    this.name = 'EnqueueAcceptanceError'
    this.outcomes = outcomes
    this.results = results
    this.numFlushed = results.length
    this.protocolFailure = protocolFailure
  }
}

async function accepted (message, MessageId, opt) {
  try {
    const confirmed = await confirmDedup(message, MessageId, opt)
    return confirmed === 0
      ? { status: 'accepted', MessageId, DeduplicationConfirmation: 'not-current' }
      : { status: 'accepted', MessageId }
  } catch {
    return { status: 'accepted', MessageId, DeduplicationConfirmation: 'unavailable' }
  }
}

async function rejected (message, reason, opt) {
  try {
    await releasePendingDedup(message, opt)
    return { status: 'rejected', reason }
  } catch {
    return { status: 'rejected', reason, DeduplicationRelease: 'unavailable' }
  }
}

async function claim (message, opt) {
  if (!opt.externalDedup) return { status: 'claimed' }
  try {
    return await claimDedup(message, opt)
  } catch {
    return { status: 'unknown', reason: 'cache-claim' }
  }
}

export async function sendMessage (qrl, queue, command, opt, messageOptions) {
  const params = { QueueUrl: qrl, ...formatMessage(command, null, opt, messageOptions) }
  const admission = await claim(params, opt)
  const fail = outcome => new EnqueueAcceptanceError([{ Id: '0', ...outcome }], [])
  if (admission.status === 'legacyDeduplicated') {
    return { MessageId: (opt.uuidFunction || uuidV1)(), Deduplicated: 'legacy' }
  }
  if (admission.status === 'deduplicated') return { MessageId: admission.MessageId, Deduplicated: 'confirmed' }
  if (admission.status !== 'claimed') throw fail(admission)

  const custody = { started: false, uncertain: false }
  let data
  try {
    data = await sendPrepared(queue, params, opt, SendMessageCommand, custody)
  } catch (error) {
    if (!opt.externalDedup) throw error
    const outcome = custody.uncertain
      ? { status: 'unknown', reason: 'send' }
      : await rejected(params, custody.started ? 'broker-request' : 'not-sent', opt)
    throw fail(outcome)
  }
  if (!hasMessageId(data?.MessageId)) throw fail({ status: 'unknown', reason: 'response' })
  const outcome = await accepted(params, data.MessageId, opt)
  if (outcome.DeduplicationConfirmation) return { ...data, DeduplicationConfirmation: outcome.DeduplicationConfirmation }
  return data
}

// Transport retries use exactly the prepared entries, including through queue recreation.
export async function sendMessageBatch (qrl, queue, messages, opt, custody = { started: false, uncertain: false }) {
  return sendPrepared(queue, { Entries: messages, QueueUrl: qrl }, opt, SendMessageBatchCommand, custody)
}

function correlate (messages, data) {
  const expected = new Set(messages.map(message => message.Id))
  const rows = new Map()
  let invalid = !data || typeof data !== 'object'
  for (const [name, success] of [['Successful', true], ['Failed', false]]) {
    const values = data?.[name]
    if (values === undefined) continue
    if (!Array.isArray(values)) {
      invalid = true
      continue
    }
    for (const row of values) {
      if (!expected.has(row?.Id)) {
        invalid = true
        continue
      }
      const wellFormed = success
        ? hasMessageId(row.MessageId)
        : typeof row.Code === 'string' && row.Code.length > 0 && typeof row.SenderFault === 'boolean'
      if (!wellFormed || rows.has(row.Id)) {
        rows.set(row.Id, null)
        invalid = true
        continue
      }
      rows.set(row.Id, { success, MessageId: row.MessageId })
    }
  }
  return { rows, invalid: invalid || rows.size !== expected.size }
}

const ownerAttributeSize = JSON.stringify({
  QdoneDeduplicationOwner: { DataType: 'String', StringValue: '0'.repeat(32) }
}).length - 1 // The attribute adds a comma inside the existing attributes object.

function createLedger (ids) {
  return {
    outcomes: new Map(ids.map(Id => [Id, { Id, status: 'notAttempted' }])),
    owners: new Map(),
    aliases: new Map(),
    resultOrder: [],
    failed: false,
    protocolFailure: false,
    requestCount: 0
  }
}

function complete (ledger, message, outcome) {
  const Id = message.Id
  const dedupId = messageDedupId(message)
  ledger.outcomes.set(Id, { Id, ...outcome, ...(dedupId ? { QdoneDeduplicationId: dedupId } : {}) })
  if (outcome.status === 'rejected' || outcome.status === 'unknown') ledger.failed = true
}

async function prepareBatch (batch, opt, ledger) {
  const ready = await Promise.all(batch.map(async message => {
    if (opt.externalDedup && opt.dedupProtocol === 'owner-v1') {
      const dedupId = messageDedupId(message)
      const owner = ledger.owners.get(dedupId)
      if (owner !== undefined) {
        ledger.aliases.set(message.Id, { owner, message })
        return false
      }
      ledger.owners.set(dedupId, message.Id)
    }
    const admission = await claim(message, opt)
    if (admission.status === 'claimed') return true
    complete(ledger, message, admission)
    return false
  }))
  return batch.filter((message, index) => ready[index])
}

async function flushBatch (qrl, queue, batch, opt, ledger) {
  const messages = await prepareBatch(batch, opt, ledger)
  if (!messages.length) return
  if (ledger.failed) {
    // This command has not started. Release only owned claims and leave its inputs unattempted.
    await Promise.allSettled(messages.map(message => releasePendingDedup(message, opt)))
    return
  }

  const custody = { started: false, uncertain: false }
  let data
  try {
    data = await sendMessageBatch(qrl, queue, messages, opt, custody)
  } catch {
    ledger.failed = true
    await Promise.all(messages.map(async message => {
      const outcome = custody.uncertain
        ? { status: 'unknown', reason: 'send' }
        : await rejected(message, custody.started ? 'broker-request' : 'not-sent', opt)
      complete(ledger, message, outcome)
    }))
    return
  }

  const { rows, invalid } = correlate(messages, data)
  if (invalid) ledger.protocolFailure = true
  if (invalid || [...rows.values()].some(row => !row?.success)) ledger.failed = true
  const definite = !invalid && !custody.uncertain && data.$metadata?.attempts === 1
  const request = ++ledger.requestCount
  await Promise.all(messages.map(async message => {
    const row = rows.get(message.Id)
    if (row?.success) {
      const outcome = await accepted(message, row.MessageId, opt)
      complete(ledger, message, outcome)
      if (opt.verbose) console.error(chalk.blue('Enqueued job ') + row.MessageId + chalk.blue(' request ' + request))
    } else {
      const outcome = row && definite
        ? await rejected(message, 'broker-entry', opt)
        : { status: 'unknown', reason: invalid ? 'response' : 'send-attempts' }
      complete(ledger, message, outcome)
    }
  }))
}

function finalize (ledger) {
  for (const { owner, message } of ledger.aliases.values()) {
    const { status, MessageId, reason } = ledger.outcomes.get(owner)
    complete(ledger, message, knownAcceptance({ status })
      ? { status: 'deduplicated', MessageId }
      : { status, ...(reason ? { reason } : {}) })
  }

  const outcomes = [...ledger.outcomes.values()]
  const results = ledger.resultOrder.flatMap(Id => {
    const outcome = ledger.outcomes.get(Id)
    if (!knownAcceptance(outcome) && (ledger.failed || outcome.status !== 'legacyDeduplicated')) return []
    const { MessageId, QdoneDeduplicationId, DeduplicationConfirmation } = outcome
    const result = { Id, MessageId: outcome.status === 'legacyDeduplicated' ? 'duplicate' : MessageId }
    if (QdoneDeduplicationId) result.QdoneDeduplicationId = QdoneDeduplicationId
    if (outcome.status === 'deduplicated') result.Deduplicated = 'confirmed'
    if (outcome.status === 'legacyDeduplicated') result.Deduplicated = 'legacy'
    if (DeduplicationConfirmation) result.DeduplicationConfirmation = DeduplicationConfirmation
    return [result]
  })
  if (ledger.failed) throw new EnqueueAcceptanceError(outcomes, results, ledger.protocolFailure)
  return { numFlushed: results.length, results }
}

export async function flushMessages (qrl, queue, opt, sendBuffer, invocation) {
  const buffer = sendBuffer[qrl] ?? []
  const ids = buffer.map(message => message.Id)
  const ledger = invocation ?? createLedger(ids)
  ledger.resultOrder.push(...ids)
  const extraSize = opt.externalDedup && opt.dedupProtocol === 'owner-v1' ? ownerAttributeSize : 0
  while (buffer.length && !ledger.failed) {
    const batch = []
    let totalSize = 0
    while (buffer.length && batch.length < 10) {
      const nextSize = JSON.stringify(buffer[0]).length + extraSize
      if (totalSize + nextSize >= 262144) break
      batch.push(buffer.shift())
      totalSize += nextSize
    }
    if (!batch.length) {
      complete(ledger, buffer[0], { status: 'rejected', reason: 'batch-size' })
      break
    }
    await flushBatch(qrl, queue, batch, opt, ledger)
  }
  if (!invocation) return finalize(ledger)
}

export async function addMessage (qrl, queue, command, messageIndex, opt, sendBuffer, messageOptions, ledger) {
  const message = formatMessage(command, messageIndex, opt, messageOptions)
  sendBuffer[qrl] = sendBuffer[qrl] || []
  sendBuffer[qrl].push(message)
  if (sendBuffer[qrl].length >= 10) {
    return flushMessages(qrl, queue, opt, sendBuffer, ledger)
  }
  return { numFlushed: 0, results: [] }
}

export async function enqueue (queue, command, options) {
  debug('enqueue(', { queue, command }, ')')
  const opt = getOptionsWithDefaults(options)
  if (opt.sentryDsn) {
    setExtra({ qdoneOperation: 'enqueue', args: { queue, command, opt: redactCacheUri(opt) } })
  }
  const qrl = await getOrCreateQueue(queue, opt)
  return sendMessage(qrl, queue, command, opt)
}

export async function enqueueBatch (pairs, options) {
  const opt = getOptionsWithDefaults(options)
  if (opt.sentryDsn) {
    setExtra({ qdoneOperation: 'enqueueBatch', args: { pairs, opt: redactCacheUri(opt) } })
  }
  const ledger = createLedger(pairs.map((pair, index) => String(index)))
  const sendBuffer = {}
  try {
    const normalizedPairs = pairs.map(({ queue, command, messageOptions }) => ({
      qname: normalizeQueueName(queue, opt),
      command,
      messageOptions: validateMessageOptions(messageOptions)
    }))
    const qnames = [...new Set(normalizedPairs.map(pair => pair.qname))]
    await Promise.all(qnames.map(qname => getOrCreateQueue(qname, opt)))
    const qrlToQname = new Map()
    for (const [index, { qname, command, messageOptions }] of normalizedPairs.entries()) {
      const qrl = await getOrCreateQueue(qname, opt)
      qrlToQname.set(qrl, qname)
      await addMessage(qrl, qname, command, index, opt, sendBuffer, messageOptions, ledger)
      if (ledger.failed) break
    }

    if (!ledger.failed) {
      const tail = await Promise.allSettled(Object.keys(sendBuffer).map(
        qrl => flushMessages(qrl, qrlToQname.get(qrl), opt, sendBuffer, ledger)
      ))
      if (tail.some(result => result.status === 'rejected')) ledger.failed = true
    }
  } catch {
    ledger.failed = true
  }
  return finalize(ledger)
}

debug('loaded')
