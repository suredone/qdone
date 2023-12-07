/* eslint-env jest */

import {
  getClient,
  resetClient,
  setCache,
  getCache
} from '../src/cache.js'

beforeEach(resetClient)
afterAll(resetClient)

const options = {
  'cache-uri': 'redis://localhost',
  'cache-ttl-seconds': 10,
  'cache-prefix': 'qdone:'
}

describe('getClient', () => {
  test('throws an error for invalid cache uri', () => {
    const badOptions = Object.assign({}, options, { 'cache-uri': 'bob://foo' })
    expect(() => getClient(badOptions)).toThrow('currently supported')
  })
  test('throws an error when --cache-uri is missing', () => {
    const badOptions = Object.assign({}, options)
    delete badOptions['cache-uri']
    expect(() => getClient(badOptions)).toThrow(/--cache-uri/)
  })
  test('supports redis:// URIs', () => {
    expect(() => getClient(options)).not.toThrow()
  })
  test('supports redis-cache:// URIs', () => {
    expect(() => getClient(
      Object.assign({}, options, { 'cache-uri': 'redis-cluster://' })
    )).not.toThrow()
  })
  test('returns an identical client on subsequent calls', () => {
    const client1 = getClient(options)
    const client2 = getClient(options)
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
