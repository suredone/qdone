/**
 * Routines for handling Sentry instrumentation
 */

import Debug from 'debug'
import { init, withScope, captureException, captureMessage } from '@sentry/node'

const debug = Debug('qdone:sentry')

let sentryWasInit = false
function ensureInit (opt) {
  if (!opt.sentryDsn) return false
  if (!sentryWasInit) {
    init({ dsn: opt.sentryDsn, traceSampleRate: 0 })
    sentryWasInit = true
  }
  return true
}

/**
 * Proactively report an event to Sentry (no thrown error required). Used for
 * command-policy violations / misconfigurations, which return a failed job
 * rather than throwing. No-op when Sentry is not configured.
 */
export async function reportEvent (opt, level, message, contexts) {
  if (!ensureInit(opt)) return
  await withScope(async function (scope) {
    scope.setLevel(level)
    if (contexts instanceof Object) {
      for (const key in contexts) scope.setContext(key, contexts[key])
    }
    captureMessage(message, level)
  })
}

export async function withSentry (callback, opt, contexts) {
  debug({ withSentry: { callback, opt, contexts } })
  // Bail if sentry isn't enabled
  if (!ensureInit(opt)) return callback()

  try {
    const result = await callback()
    debug({ result })
    return result
  } catch (err) {
    debug({ err })
    await withScope(async function (scope) {
      scope.setContext('opt', opt)
      if (contexts instanceof Object) {
        for (const key in contexts) scope.setContext(key, contexts[key])
        if (err.stdout && err.stderr) {
          const { stdout, stderr } = err
          scope.setContext('IO', { stdout, stderr })
        }
      }
      const sentryResult = await captureException(err)
      debug({ sentryResult })
    })
    throw err
  }
}

debug('loaded')
