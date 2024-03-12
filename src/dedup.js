import { getCacheClient } from './cache.js'
import Debug from 'debug'
const debug = Debug('qdone:dedup')

export function getCacheKey (content, opt) {
  const cacheKey = opt.cachePrefix + 'dedup:' + content.trim()
  debug({ action: 'shouldEnqueue', cacheKey })
  return cacheKey
}
export async function dedupShouldEnqueue (content, opt) {
  if (opt.dedupMethod === 'redis') {
    const client = getCacheClient(opt)
    const cacheKey = getCacheKey(content, opt)
    const result = await client.incr(cacheKey)
    debug({ action: 'shouldEnqueue', cacheKey, result })
    if (result === 1) {
      // We won, now make sure it expires
      await client.expire(cacheKey, opt.dedupPeriod)
      return true
    }
    return false
  } else {
    return true
  }
}

export async function dedupShouldEnqueueMulti (contentArray, opt) {
  const results = {}
  if (opt.dedupMethod === 'redis') {
    // Increment all
    const incrPipeline = getCacheClient().pipeline()
    for (const content of contentArray) {
      const cacheKey = getCacheKey(content, opt)
      incrPipeline.incr(cacheKey)
    }
    const responses = await incrPipeline.exec()

    // Interpret responses and expire keys for races we won
    const expirePipeline = getCacheClient().pipeline()
    for (let i = 0; i < contentArray.length; i++) {
      const content = contentArray[i]
      const response = responses[i]
      const cacheKey = getCacheKey(content, opt)
      if (response === 1) {
        results[content] = true
        expirePipeline.expire(cacheKey, opt.dedupPeriod)
      } else {
        results[content] = false
      }
    }
    await await expirePipeline.exec()
  } else {
    // If we're not using redis, send everything
    for (const content of contentArray) results[content] = true
  }
  return results
}

export async function dedupSuccessfullyProcessed (content, opt) {
  if (opt.dedupMethod === 'redis') {
    const client = getCacheClient(opt)
    const cacheKey = opt.cachePrefix + ':dedup:' + content.trim()
    debug({ action: 'shouldProcess', cacheKey })
    await client.del(cacheKey)
  }
}

export async function dedupSuccessfullyProcessedMulti (contentArray, opt) {
  if (opt.dedupMethod === 'redis') {
    const delPipeline = getCacheClient().pipeline()
    for (const content of contentArray) {
      const cacheKey = getCacheKey(content, opt)
      delPipeline.del(cacheKey)
    }
    await delPipeline.exec()
  }
}
