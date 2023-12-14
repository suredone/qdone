/**
 * Default options for qdone. Accepts a command line options object and
 * returns nicely-named options.
 */
import { v1 as uuidv1 } from 'uuid'

export const defaults = Object.freeze({
  prefix: 'qdone_',
  failSuffix: '_failed',
  region: 'us-east-1',
  quiet: false,
  verbose: false,
  cache: false,
  cachePrefix: 'qdone:',
  cacheTtlSeconds: 10,
  fifo: false,
  groupId: uuidv1(),
  groupIdPerMessage: false,
  deduplicationId: uuidv1(),
  messageRetentionPeriod: 1209600,
  delay: 0,
  dlq: false,
  dlqSuffix: '_dead',
  dlqAfter: 3
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
    prefix: options.prefix === '' ? options.prefix : defaults.prefix,
    failSuffix: options.failSuffix || options['fail-suffix'] || defaults.failSuffix,
    region: options.region || process.env.AWS_REGION || defaults.region,
    quiet: options.quiet || defaults.quiet,
    verbose: options.verbose || defaults.verbose,
    cachePrefix: options.cachePrefix || options['cache-prefix'] || defaults.cachePrefix,
    cacheTtlSeconds: options.cacheTtlSeconds || options['cache-ttl-seconds'] || defaults.cacheTtlSeconds,
    fifo: options.fifo || defaults.fifo,
    groupId: options.groupId || options['group-id'] || defaults.groupId,
    groupIdPerMessage: false,
    deduplicationId: options.deduplicationId || options['deduplication-id'] || defaults.deduplicationId,
    messageRetentionPeriod: options['message-retention-period'] || defaults.messageRetentionPeriod,
    delay: options.delay || defaults.delay,
    dlq: dlq || defaults.dlq,
    dlqSuffix: options.dlqSuffix || options['dlq-suffix'] || defaults.dlqSuffix,
    dlqAfter: options.dlqAfter || options['dlq-after'] || defaults.dlqAfter,
    sentryDsn: options.sentryDsn || options['sentry-dsn']
  }

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
