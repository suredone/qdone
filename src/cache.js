import Redis from 'ioredis'
import { URL } from 'url'
import Debug from 'debug'
const debug = Debug('qdone:cache')

class UsageError extends Error {}

let client

/**
 * Build the ioredis Cluster options for a redis-cluster:// or rediss-cluster:// URI.
 *
 * The seed node is handed over as host and port with any URI password in the node options, because
 * ioredis does not carry credentials from a node URL string reliably. With TLS the cluster must
 * connect to the node hostnames it announces instead of resolving them first, since the
 * certificates name the hosts (ioredis' documented ElastiCache handling).
 */
export function clusterOptionsFromUri (url) {
  const tls = url.protocol === 'rediss-cluster:'
  const password = url.password ? decodeURIComponent(url.password) : undefined
  const clusterOptions = { slotsRefreshInterval: 60 * 1000, redisOptions: {} }
  if (tls) {
    clusterOptions.dnsLookup = (address, callback) => callback(null, address)
    clusterOptions.redisOptions.tls = {}
  }
  if (password) clusterOptions.redisOptions.password = password
  return {
    nodes: [{ host: url.hostname, port: Number(url.port) || 6379 }],
    clusterOptions
  }
}

/**
 * Internal function to setup redis client. Parses URI to figure out
 * how to connect: redis:// and rediss:// for a single node (ioredis reads TLS and
 * credentials from the URI), redis-cluster:// and rediss-cluster:// for a cluster.
 */
export function getCacheClient (opt) {
  const RedisClass = opt.Redis || Redis
  if (client) {
    return client
  } else if (opt.cacheUri) {
    const url = new URL(opt.cacheUri)
    if (url.protocol === 'redis:' || url.protocol === 'rediss:') {
      client = new RedisClass(url.toString())
    } else if (url.protocol === 'redis-cluster:' || url.protocol === 'rediss-cluster:') {
      const { nodes, clusterOptions } = clusterOptionsFromUri(url)
      client = new RedisClass.Cluster(nodes, clusterOptions)
    } else {
      throw new UsageError(`Only redis://, rediss://, redis-cluster:// or rediss-cluster:// URLs are supported. Got: ${url.protocol}`)
    }
    return client
  } else {
    throw new UsageError('Caching requires the --cache-uri option')
  }
}

export function shutdownCache () {
  if (client) client.quit()
  client = undefined
}

/**
 * Returns a promise for the item. Resolves to false if cache is empty, object
 * if it is found.
 */
export async function getCache (key, opt) {
  const client = getCacheClient(opt)
  const cacheKey = opt.cachePrefix + key
  debug({ action: 'getCache', cacheKey })
  const result = await client.get(cacheKey)
  debug({ action: 'getCache got', cacheKey, result })
  return result ? JSON.parse(result) : undefined
}

/**
 * Returns a promise for the status. Encodes object as JSON
 */
export async function setCache (key, value, opt) {
  const client = getCacheClient(opt)
  const encoded = JSON.stringify(value)
  const cacheKey = opt.cachePrefix + key
  debug({ action: 'setCache', cacheKey, value })
  return client.setex(cacheKey, opt.cacheTtlSeconds, encoded)
}

debug('loaded')
