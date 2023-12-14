/**
 * Routines for handling Sentry instrumentation
 */

import Debug from 'debug'
import { init, captureException } from '@sentry/node'

const debug = Debug('qdone:sentry')

let sentryWasInit = false
export async function withSentry (operationName, callback, opt) {
  // Bail if sentry isn't enabled
  if (!opt.sentryDsn) return callback()

  // Init sentry if it's not already
  if (!sentryWasInit) {
    init({ dsn: opt.sentryDsn, traceSampleRate: 0 })
    sentryWasInit = true
  }
  // const transaction = startTransaction({
  //   op: operationName, name: callback
  // })
  try {
    const result = await callback()
    debug({ result })
    return result
  } catch (err) {
    debug({ err })
    const sentryResult = await captureException(err)
    debug({ sentryResult })
    throw err
  } finally {
    // const txnResult = await transaction.finish()
    // debug({ txnResult })
  }
}
