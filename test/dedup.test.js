/* eslint-env jest */

import Redis from 'ioredis-mock'
import { getOptionsWithDefaults } from '../src/defaults.js'
import { shutdownCache, getCacheClient } from '../src/cache.js'
import { getCacheKey, dedupShouldEnqueue } from '../src/dedup.js'

const options = {
  cacheUri: 'redis://localhost',
  cacheTtlSeconds: 10,
  cachePrefix: 'qdone:'
}

beforeEach(shutdownCache)
afterEach(async () => getCacheClient(options).flushall())
afterAll(shutdownCache)

describe('getCacheKey', () => {
  test('creates key based on prefix', () => {
    const opt = getOptionsWithDefaults(options)
    opt.Redis = new Redis({ data: {} })
    expect(getCacheKey('ls -al', opt)).toEqual('qdone:dedup:ls -al')
  })
  test('ignores whitespace', () => {
    const opt = getOptionsWithDefaults(options)
    expect(getCacheKey(' ls -al    \t', opt)).toEqual('qdone:dedup:ls -al')
  })
})

describe('dedupShouldEnqueue', () => {
  test('resolves true if custom dedup is not enabled', async () => {
    const opt = getOptionsWithDefaults(options)
    opt.Redis = Redis
    await expect(dedupShouldEnqueue('ls -al', opt)).resolves.toEqual(true)
  })
  test('resolves true if custom dedup is enabled and cache is empty', async () => {
    const opt = getOptionsWithDefaults(Object.assign({}, options, { dedupMethod: 'redis' }))
    opt.Redis = Redis
    await expect(dedupShouldEnqueue('ls -al', opt)).resolves.toEqual(true)
  })
  test('resolves false if custom dedup is enabled and cache is full', async () => {
    const opt = getOptionsWithDefaults(Object.assign({}, options, { dedupMethod: 'redis' }))
    opt.Redis = Redis
    await expect(dedupShouldEnqueue('ls -al', opt)).resolves.toEqual(true)
    await expect(dedupShouldEnqueue('ls -al', opt)).resolves.toEqual(false)
  })
})
