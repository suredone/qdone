/**
 * Default options for qdone. Accepts a command line options object and
 * returns nicely-named options.
 */
import { v1 as uuidv1 } from 'uuid'

export const defaults = Object.freeze({
  // Shared
  prefix: 'qdone_',
  failSuffix: '_failed',
  region: 'us-east-1',
  quiet: false,
  verbose: false,
  cache: false,
  cacheUri: undefined,
  cachePrefix: 'qdone:',
  cacheTtlSeconds: 10,
  fifo: false,
  disableLog: false,
  includeFailed: false,

  // Enqueue
  groupId: uuidv1(),
  groupIdPerMessage: false,
  deduplicationId: undefined,
  messageRetentionPeriod: 1209600,
  delay: 0,
  sendRetries: 6,
  failDelay: 0,
  dlq: false,
  dlqSuffix: '_dead',
  dlqAfter: 3,

  // Worker
  waitTime: 20,
  killAfter: 30,
  archive: false,
  activeOnly: false,

  // Idle Queues
  idleFor: 60,
  delete: false,
  unpair: false
})

/**
 * This function should be called by each exposed API entry point on the
 * options passed in from the caller. It supports options named in camelCase
 * and also in command-line-style returned by our parsers.
 *
 * By convention, we name the variable "options" if it comes from the user
 * and "opt" if it has already passed through this function.
 */
export function getOptionsWithDefaults (options) {
  // For API invocations don't force caller to supply default options
  if (!options) options = {}

  // Activate DLQ if any option is set
  const dlq = options.dlq || !!(options['dlq-suffix'] || options['dlq-after'] || options['dlq-name'])

  const opt = {
    // Shared
    prefix: options.prefix === '' ? options.prefix : (options.prefix || defaults.prefix),
    failSuffix: options.failSuffix || options['fail-suffix'] || defaults.failSuffix,
    region: options.region || process.env.AWS_REGION || defaults.region,
    quiet: options.quiet || defaults.quiet,
    verbose: options.verbose || defaults.verbose,
    fifo: options.fifo || defaults.fifo,
    sentryDsn: options.sentryDsn || options['sentry-dsn'],
    disableLog: options.disableLog || options['disable-log'] || defaults.disableLog,

    // Cache
    cacheUri: options.cacheUri || options['cache-uri'] || defaults.cacheUri,
    cachePrefix: options.cachePrefix || options['cache-prefix'] || defaults.cachePrefix,
    cacheTtlSeconds: options.cacheTtlSeconds || options['cache-ttl-seconds'] || defaults.cacheTtlSeconds,

    // Enqueue
    groupId: options.groupId || options['group-id'] || defaults.groupId,
    groupIdPerMessage: false,
    deduplicationId: options.deduplicationId || options['deduplication-id'] || defaults.deduplicationId,
    messageRetentionPeriod: options.messageRetentionPeriod || options['message-retention-period'] || defaults.messageRetentionPeriod,
    delay: options.delay || defaults.delay,
    sendRetries: options['send-retries'] || defaults.sendRetries,
    failDelay: options.failDelay || options['fail-delay'] || defaults.failDelay,
    dlq: dlq || defaults.dlq,
    dlqSuffix: options.dlqSuffix || options['dlq-suffix'] || defaults.dlqSuffix,
    dlqAfter: options.dlqAfter || options['dlq-after'] || defaults.dlqAfter,
    tags: options.tags || undefined,

    // Worker
    waitTime: options.waitTime || options['wait-time'] || defaults.waitTime,
    killAfter: options.killAfter || options['kill-after'] || defaults.killAfter,
    archive: options.archive || defaults.archive,
    activeOnly: options.activeOnly || options['active-only'] || defaults.activeOnly,
    includeFailed: options.includeFailed || options['include-failed'] || defaults.includeFailed,

    // Idle Queues
    idleFor: options.idleFor || options['idle-for'] || defaults.idleFor,
    delete: options.delete || defaults.delete,
    unpair: options.delete || defaults.unpair
  }
  process.env.AWS_REGION = opt.region

  // TODO: validate options
  return opt
}

export function setupAWS (options) {
  const opt = getOptionsWithDefaults(options)
  process.env.AWS_REGION = opt.region
}

export function setupVerbose (options) {
  const verbose = options.verbose || (process.stderr.isTTY && !options.quiet)
  const quiet = options.quiet || (!process.stderr.isTTY && !options.verbose)
  options.verbose = verbose
  options.quiet = quiet
}
