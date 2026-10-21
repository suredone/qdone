/* eslint-env jest */

import Redis from 'ioredis-mock'
import { URL } from 'url'
import {
  clusterOptionsFromUri,
  getCacheClient,
  shutdownCache,
  setCache,
  getCache
} from '../src/cache.js'

const opt = {
  cacheUri: 'redis://localhost',
  cacheTtlSeconds: 10,
  cachePrefix: 'qdone:',
  Redis
}

beforeEach(shutdownCache)
afterEach(async () => getCacheClient(opt).flushall())
afterAll(shutdownCache)

describe('getCacheClient', () => {
  test('throws an error for invalid cache uri', () => {
    const badOpt = Object.assign({}, opt, { cacheUri: 'bob://foo' })
    expect(() => getCacheClient(badOpt)).toThrow(/rediss-cluster:\/\/ URLs are supported\. Got: bob:/)
  })
  test('throws an error when --cache-uri is missing', () => {
    const badOpt = Object.assign({}, opt)
    delete badOpt.cacheUri
    expect(() => getCacheClient(badOpt)).toThrow(/--cache-uri/)
  })
  test('supports redis:// URIs', () => {
    expect(() => getCacheClient(opt)).not.toThrow()
  })
  test('supports rediss:// URIs', () => {
    expect(() => getCacheClient(
      Object.assign({}, opt, { cacheUri: 'rediss://localhost:6379' })
    )).not.toThrow()
  })
  test('supports redis-cluster:// URIs', () => {
    expect(() => getCacheClient(
      Object.assign({}, opt, { cacheUri: 'redis-cluster://localhost:6379' })
    )).not.toThrow()
  })
  test('supports rediss-cluster:// URIs', () => {
    expect(() => getCacheClient(
      Object.assign({}, opt, { cacheUri: 'rediss-cluster://localhost:6379' })
    )).not.toThrow()
  })
  test('hands the seed node, TLS and password to the cluster client', () => {
    const calls = []
    class RedisStub {
      static Cluster = class {
        constructor (nodes, options) { calls.push({ nodes, options }) }
        quit () {}
        flushall () {}
      }
    }
    getCacheClient(Object.assign({}, opt, {
      Redis: RedisStub,
      cacheUri: 'rediss-cluster://:s3cret%40pass@clustercfg.example.cache.amazonaws.com:6380'
    }))
    expect(calls).toHaveLength(1)
    expect(calls[0].nodes).toEqual([{ host: 'clustercfg.example.cache.amazonaws.com', port: 6380 }])
    expect(calls[0].options.redisOptions.tls).toEqual({})
    expect(calls[0].options.redisOptions.password).toBe('s3cret@pass')
    expect(calls[0].options.slotsRefreshInterval).toBe(60 * 1000)
    shutdownCache()
  })
  test('returns an identical client on subsequent calls', () => {
    const client1 = getCacheClient(opt)
    const client2 = getCacheClient(opt)
    expect(client1).toEqual(client2)
  })
})

describe('clusterOptionsFromUri', () => {
  test('connects to announced node names unchanged under TLS, so certificates verify', () => {
    const { nodes, clusterOptions } = clusterOptionsFromUri(new URL('rediss-cluster://cache.example:6379'))
    expect(nodes).toEqual([{ host: 'cache.example', port: 6379 }])
    const resolved = []
    clusterOptions.dnsLookup('node-0001.cache.example', (error, address) => resolved.push([error, address]))
    expect(resolved).toEqual([[null, 'node-0001.cache.example']])
  })
  test('leaves a plaintext cluster without TLS, lookup override or password', () => {
    const { nodes, clusterOptions } = clusterOptionsFromUri(new URL('redis-cluster://cache.example'))
    expect(nodes).toEqual([{ host: 'cache.example', port: 6379 }])
    expect(clusterOptions.dnsLookup).toBeUndefined()
    expect(clusterOptions.redisOptions).toEqual({})
  })
})

describe('setCache', function () {
  test('should successfully set a value', async () => {
    await expect(
      setCache('test', { one: 1 }, opt)
    ).resolves.toBe('OK')
  })
})

describe('getCache', function () {
  test('should return the same value that was set', async () => {
    await setCache('test', { one: 1 }, opt)
    await expect(
      getCache('test', opt)
    ).resolves.toEqual({ one: 1 })
  })
  test('should return undefined for nonexistent keys', async () => {
    await expect(
      getCache('supercalafragalisticexpealadocious', opt)
    ).resolves.toBeUndefined()
  })
})
