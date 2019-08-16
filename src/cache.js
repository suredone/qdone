
const Redis = require('ioredis')
const { URL } = require('url')
const debug = require('debug')('qdone:cache')

class UsageError extends Error {}

let client

/**
 * Internal function to setup redis client. Parses URI to figure out
 * how to connect.
 */
function getClient (options) {
  if (client) {
    return client
  } else if (options['cache-uri']) {
    const url = new URL(options['cache-uri'])
    if (url.protocol === 'redis:') {
      client = new Redis(url.toString())
    } else if (url.protocol === 'redis-cluster:') {
      url.protocol = 'redis:'
      client = new Redis.Cluster([url.toString()], { slotsRefreshInterval: 60 * 1000 })
    } else {
      throw new UsageError(`Only redis:// or redis-cluster:// URLs are currently supported. Got: ${url.protocol}`)
    }
    // setTimeout(resetClient, 10000)
    return client
  } else {
    throw new UsageError('Caching requires the --cache-uri option')
  }
}

function resetClient () {
  if (client) client.quit()
  client = undefined
}

/**
 * Returns a promise for the item. Resolves to false if cache is empty, object
 * if it is found.
 */
function getCache (key, options) {
  const client = getClient(options)
  const cacheKey = options['cache-prefix'] + key
  debug({ action: 'getCache', cacheKey })
  return client.get(cacheKey).then(result => {
    debug({ action: 'getCache got', cacheKey, result })
    return result ? JSON.parse(result) : undefined
  })
}

/**
 * Returns a promise for the status. Encodes object as JSON
 */
function setCache (key, value, options) {
  const client = getClient(options)
  const encoded = JSON.stringify(value)
  const cacheKey = options['cache-prefix'] + key
  debug({ action: 'setCache', cacheKey, value })
  return client.setex(cacheKey, options['cache-ttl-seconds'], encoded)
}

module.exports = { getCache, setCache, getClient, resetClient }

debug('loaded')
