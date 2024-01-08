/* eslint-env jest */

import {
  getCacheClient,
  shutdownCache,
  setCache,
  getCache
} from '../src/cache.js'

beforeEach(shutdownCache)
afterAll(shutdownCache)

const options = {
  cacheUri: 'redis://localhost',
  cacheTtlSeconds: 10,
  cachePrefix: 'qdone:'
}

describe('getCacheClient', () => {
  test('throws an error for invalid cache uri', () => {
    const badOptions = Object.assign({}, options, { cacheUri: 'bob://foo' })
    expect(() => getCacheClient(badOptions)).toThrow('currently supported')
  })
  test('throws an error when --cache-uri is missing', () => {
    const badOptions = Object.assign({}, options)
    delete badOptions.cacheUri
    expect(() => getCacheClient(badOptions)).toThrow(/--cache-uri/)
  })
  test('supports redis:// URIs', () => {
    expect(() => getCacheClient(options)).not.toThrow()
  })
  test('supports redis-cache:// URIs', () => {
    expect(() => getCacheClient(
      Object.assign({}, options, { cacheUri: 'redis-cluster://' })
    )).not.toThrow()
  })
  test('returns an identical client on subsequent calls', () => {
    const client1 = getCacheClient(options)
    const client2 = getCacheClient(options)
    expect(client1).toEqual(client2)
  })
})

describe('setCache', function () {
  test('should successfully set a value', async () => {
    await expect(
      setCache('test', { one: 1 }, options)
    ).resolves.toBe('OK')
  })
})

describe('getCache', function () {
  test('should return the same value that was set', async () => {
    await setCache('test', { one: 1 }, options)
    await expect(
      getCache('test', options)
    ).resolves.toEqual({ one: 1 })
  })
  test('should return undefined for nonexistent keys', async () => {
    await expect(
      getCache('supercalafragalisticexpealadocious', options)
    ).resolves.toBeUndefined()
  })
})
