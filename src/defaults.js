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
  includeDead: false,
  externalDedup: false,
  dedupPeriod: 60 * 5,
  dedupStats: false,

  // Enqueue
  groupId: uuidv1(),
  groupIdPerMessage: false,
  deduplicationId: undefined,
  dedupIdPerMessage: false,
  messageRetentionPeriod: 1209600,
  delay: 0,
  sendRetries: 6,
  failDelay: 120,
  dlq: true,
  dlqSuffix: '_dead',
  dlqAfter: 3,

  // Worker
  waitTime: 20,
  killAfter: 30,
  archive: false,
  activeOnly: false,
  maxConcurrentJobs: 100,
  maxMemoryPercent: 70,

  // Idle Queues
  idleFor: 60,
  delete: false,

  // Check
  create: false,
  overwrite: false
})

export function validateInteger (opt, name) {
  const parsed = parseInt(opt[name], 10)
  if (isNaN(parsed)) throw new Error(`${name} needs to be an integer.`)
  return parsed
}

export function validateQueueName (opt, name) {
  if (typeof name !== 'string') throw new Error(`${name} must be a string.`)
  if (!name.match(/^[a-z0-9-_]+$/i)) throw new Error(`${name} can contain only numbers, letters, hypens and underscores.`)
  return name
}

export function validateMessageOptions (messageOptions) {
  const validKeys = ['deduplicationId', 'groupId']
  if (typeof messageOptions === 'object' &&
      !Array.isArray(messageOptions) &&
      messageOptions !== null) {
    for (const key in messageOptions) {
      if (!validKeys.includes(key)) throw new Error(`Invalid message option ${key}`)
    }
    return messageOptions
  }
  return {}
}

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
  const dlq = options.dlq || !!(options['dlq-suffix'] || options['dlq-after'] || options['dlq-name'] || options.dlqSuffix || options.dlqAfter || options.dlqName)

  const opt = {
    // Shared
    prefix: options.prefix === '' ? options.prefix : (options.prefix || process.env.QDONE_PREFIX || defaults.prefix),
    failSuffix: options.failSuffix || options['fail-suffix'] || process.env.QDONE_FAIL_SUFFIX || defaults.failSuffix,
    region: options.region || process.env.QDONE_REGION || process.env.AWS_REGION || defaults.region,
    quiet: options.quiet || process.env.QDONE_QUIET === 'true' || defaults.quiet,
    verbose: options.verbose || process.env.QDONE_VERBOSE === 'true' || defaults.verbose,
    fifo: options.fifo || process.env.QDONE_FIFO === 'true' || defaults.fifo,
    sentryDsn: options.sentryDsn || options['sentry-dsn'] || process.env.QDONE_SENTRY_DSN,
    disableLog: options.disableLog || options['disable-log'] || process.env.QDONE_DISABLE_LOG === 'true' || defaults.disableLog,
    includeFailed: options.includeFailed || options['include-failed'] || process.env.QDONE_INCLUDE_FAILED === 'true' || defaults.includeFailed,
    includeDead: options.includeDead || options['include-dead'] || process.env.QDONE_INCLUDE_DEAD === 'true' || defaults.includeDead,
    externalDedup: options.externalDedup || options['external-dedup'] || process.env.QDONE_EXTERNAL_DEDUP === 'true' || defaults.externalDedup,
    dedupPeriod: options.dedupPeriod || options['dedup-period'] || process.env.QDONE_DEDUP_PERIOD || defaults.dedupPeriod,
    dedupStats: options.dedupStats || options['dedup-stats'] || process.env.QDONE_DEDUP_STATS === 'true' || defaults.dedupStats,

    // Cache
    cacheUri: options.cacheUri || options['cache-uri'] || process.env.QDONE_CACHE_URI || defaults.cacheUri,
    cachePrefix: options.cachePrefix || options['cache-prefix'] || process.env.QDONE_CACHE_PREFIX || defaults.cachePrefix,
    cacheTtlSeconds: options.cacheTtlSeconds || options['cache-ttl-seconds'] || process.env.QDONE_CACHE_TTL_SECONDS || defaults.cacheTtlSeconds,

    // Enqueue
    groupId: options.groupId || options['group-id'] || process.env.QDONE_GROUP_ID || defaults.groupId,
    groupIdPerMessage: false,
    deduplicationId: options.deduplicationId || options['deduplication-id'] || process.env.QDONE_DEDUPLICATION_ID || defaults.deduplicationId,
    dedupIdPerMessage: options.dedupIdPerMessage || options['dedup-id-per-message'] || process.env.QDONE_DEDUP_ID_PER_MESSAGE === 'true' || defaults.dedupIdPerMessage,
    messageRetentionPeriod: options.messageRetentionPeriod || options['message-retention-period'] || process.env.QDONE_MESSAGE_RETENTION_PERIOD || defaults.messageRetentionPeriod,
    delay: options.delay || process.env.QDONE_DELAY || defaults.delay,
    sendRetries: options.sendRetries || options['send-retries'] || process.env.QDONE_SEND_RETRIES || defaults.sendRetries,
    failDelay: options.failDelay || options['fail-delay'] || process.env.QDONE_FAIL_DELAY || defaults.failDelay,
    dlq: dlq === false ? false : defaults.dlq,
    dlqSuffix: options.dlqSuffix || options['dlq-suffix'] || process.env.QDONE_DLQ_SUFFIX || defaults.dlqSuffix,
    dlqAfter: options.dlqAfter || options['dlq-after'] || process.env.QDONE_DLQ_AFTER || defaults.dlqAfter,
    tags: options.tags || undefined,

    // Worker
    waitTime: options.waitTime || options['wait-time'] || process.env.QDONE_WAIT_TIME || defaults.waitTime,
    killAfter: options.killAfter || options['kill-after'] || process.env.QDONE_KILL_AFTER || defaults.killAfter,
    archive: options.archive || process.env.QDONE_ARCHIVE === 'true' || defaults.archive,
    activeOnly: options.activeOnly || options['active-only'] || process.env.QDONE_ACTIVE_ONLY === 'true' || defaults.activeOnly,
    maxConcurrentJobs: options.maxConcurrentJobs || process.env.QDONE_MAX_CONCURRENT_JOBS || defaults.maxConcurrentJobs,
    maxMemoryPercent: options.maxMemoryPercent || process.env.QDONE_MAX_MEMORY_PERCENT || defaults.maxMemoryPercent,

    // Idle Queues
    idleFor: options.idleFor || options['idle-for'] || process.env.QDONE_IDLE_FOR || defaults.idleFor,
    delete: options.delete || process.env.QDONE_DELETE === 'true' || defaults.delete,

    // Check
    create: options.create || process.env.QDONE_CREATE === 'true' || defaults.create,
    overwrite: options.overwrite || process.env.QDONE_OVERWRITE === 'true' || defaults.overwrite
  }

  // Setting this env here means we don't have to in AWS SDK constructors
  process.env.AWS_REGION = opt.region

  // Validation
  opt.cacheTtlSeconds = validateInteger(opt, 'cacheTtlSeconds')
  opt.messageRetentionPeriod = validateInteger(opt, 'messageRetentionPeriod')
  opt.delay = validateInteger(opt, 'delay')
  opt.sendRetries = validateInteger(opt, 'sendRetries')
  opt.dedupPeriod = validateInteger(opt, 'dedupPeriod')
  opt.failDelay = validateInteger(opt, 'failDelay')
  opt.dlqAfter = validateInteger(opt, 'dlqAfter')
  opt.waitTime = validateInteger(opt, 'waitTime')
  opt.killAfter = validateInteger(opt, 'killAfter')
  opt.maxConcurrentJobs = validateInteger(opt, 'maxConcurrentJobs')
  opt.maxMemoryPercent = validateInteger(opt, 'maxMemoryPercent')
  opt.idleFor = validateInteger(opt, 'idleFor')

  validateQueueName(opt, 'region')
  validateQueueName(opt, 'prefix')
  validateQueueName(opt, 'failSuffix')
  validateQueueName(opt, 'dlqSuffix')

  // Validate dedup args
  if (opt.externalDedup && !opt.cacheUri) throw new Error('--external-dedup requires the --cache-uri argument')
  if (opt.externalDedup && (opt.dedupPeriod < 1)) throw new Error('--external-dedup of redis requires a --dedup-period > 1 second')
  if (opt.dedupIdPerMessage && opt.deduplicationId) throw new Error('Use either --deduplication-id or --dedup-id-per-message but not both')

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
