/* eslint-env jest */

import Redis from 'ioredis-mock'
import {
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
    expect(() => getCacheClient(badOpt)).toThrow('currently supported')
  })
  test('throws an error when --cache-uri is missing', () => {
    const badOpt = Object.assign({}, opt)
    delete badOpt.cacheUri
    expect(() => getCacheClient(badOpt)).toThrow(/--cache-uri/)
  })
  test('supports redis:// URIs', () => {
    expect(() => getCacheClient(opt)).not.toThrow()
  })
  test('supports redis-cache:// URIs', () => {
    expect(() => getCacheClient(
      Object.assign({}, opt, { cacheUri: 'redis-cluster://' })
    )).not.toThrow()
  })
  test('returns an identical client on subsequent calls', () => {
    const client1 = getCacheClient(opt)
    const client2 = getCacheClient(opt)
    expect(client1).toEqual(client2)
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
