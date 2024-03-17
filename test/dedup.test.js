/* eslint-env jest */

import Redis from 'ioredis-mock'
import { getOptionsWithDefaults } from '../src/defaults.js'
import { shutdownCache, getCacheClient } from '../src/cache.js'
import {
  getCacheKey,
  getDeduplicationId,
  addDedupParamsToMessage,
  calculateDedupParams,
  dedupShouldEnqueue,
  dedupShouldEnqueueMulti
} from '../src/dedup.js'

const options = {
  cacheUri: 'redis://localhost',
  cacheTtlSeconds: 10,
  cachePrefix: 'qdone:'
}

beforeEach(shutdownCache)
afterEach(async () => getCacheClient(options).flushall())
afterAll(shutdownCache)

describe('getDeduplicationId', () => {
  test('allows allowed characters', () => {
    const opt = getOptionsWithDefaults(options)
    expect(getDeduplicationId('a-zA-Z0-9!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~', opt)).toEqual('a-zA-Z0-9!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~')
  })
  test('converts disallowed characters', () => {
    const opt = getOptionsWithDefaults(options)
    expect(getDeduplicationId('éø¢‡ asdf', opt)).toEqual('_____asdf')
  })
  test('creates id based on sha1 when content is too long', () => {
    const opt = getOptionsWithDefaults(options)
    expect(getDeduplicationId('asdf '.repeat(1000), opt)).toEqual('asdf_asdf_asdf_asdf_asdf_asdf_asdf_asdf_asdf_asdf_asdf_asdf_asdf_asdf_asdf_asdf_...sha1:ee08dd2964d1907d80f1ef743a595b5834c31170')
  })
  test('ignores whitespace', () => {
    const opt = getOptionsWithDefaults(options)
    expect(getDeduplicationId(' ls -al    \t', opt)).toEqual('ls_-al')
  })
})

describe('getCacheKey', () => {
  test('works with default prefix', () => {
    const opt = getOptionsWithDefaults(options)
    expect(getCacheKey('key', opt)).toEqual('qdone:dedup:key')
  })
  test('works with passed prefix', () => {
    const opt = getOptionsWithDefaults({ ...options, cachePrefix: 'different:' })
    expect(getCacheKey('key', opt)).toEqual('different:dedup:key')
  })
})

describe('addDedupParamsToMessage', () => {
  test('leaves non-fifo messages untouched', () => {
    const opt = getOptionsWithDefaults({ ...options, fifo: false })
    const message = { MessageBody: 'test' }
    expect(addDedupParamsToMessage(message, opt)).toEqual({ ...message })
  })
  test('adds params to fifo messages', () => {
    const opt = getOptionsWithDefaults({ ...options, fifo: true })
    const message = { MessageBody: 'test' }
    expect(addDedupParamsToMessage(message, opt)).toEqual({ ...message, MessageDeduplicationId: 'test' })
  })
  test('uses passed deduplication id when present', () => {
    const opt = getOptionsWithDefaults({ ...options, fifo: true, deduplicationId: 'foo' })
    const message = { MessageBody: 'test' }
    expect(addDedupParamsToMessage(message, opt)).toEqual({ ...message, MessageDeduplicationId: 'foo' })
  })
  test('uses unique deduplication id when per message is requested', () => {
    const opt = getOptionsWithDefaults({ ...options, fifo: true, dedupIdPerMessage: true })
    opt.uuidFunction = () => 'bar'
    const message = { MessageBody: 'test' }
    expect(addDedupParamsToMessage(message, opt)).toEqual({ ...message, MessageDeduplicationId: 'bar' })
  })
  test('adds MessageAttribute when externalDedup is used', () => {
    const opt = getOptionsWithDefaults({ ...options, fifo: true, externalDedup: true })
    const message = { MessageBody: 'test' }
    const expected = {
      ...message,
      MessageDeduplicationId: 'test',
      MessageAttributes: {
        QdoneDeduplicationId: {
          DataType: 'String',
          StringValue: 'test'
        }
      }
    }
    expect(addDedupParamsToMessage(message, opt)).toEqual(expected)
  })
})

describe('calculateDedupParams', () => {
  test('calculates expected values when dedupPeriod is longer than min', async () => {
    const dedupPeriod = 60 * 60
    const opt = getOptionsWithDefaults({ ...options, dedupPeriod })
    const timestamp = new Date().getTime()
    const expected = { timestamp, minDedupPeriod: 360, dedupPeriod, canExpireAfter: Math.round(timestamp / 1000.0 + 360) }
    expect(calculateDedupParams(opt)).toEqual(expected)
  })
})

describe('dedupShouldEnqueue', () => {
  test('resolves true if custom dedup is not enabled', async () => {
    const opt = getOptionsWithDefaults(options)
    opt.Redis = Redis
    const message = { MessageBody: 'ls -al' }
    await expect(dedupShouldEnqueue(message, opt)).resolves.toEqual(true)
  })
  test('resolves true if custom dedup is enabled and cache is empty', async () => {
    const opt = getOptionsWithDefaults(Object.assign({}, options, { externalDedup: true }))
    opt.Redis = Redis
    const message = { MessageBody: 'ls -al' }
    await expect(dedupShouldEnqueue(message, opt)).resolves.toEqual(true)
  })
  test('resolves false if custom dedup is enabled and cache is full', async () => {
    const opt = getOptionsWithDefaults(Object.assign({}, options, { externalDedup: true }))
    opt.Redis = Redis
    const message = { MessageBody: 'ls -al' }
    await expect(dedupShouldEnqueue(message, opt)).resolves.toEqual(true)
    await expect(dedupShouldEnqueue(message, opt)).resolves.toEqual(false)
  })
})

describe('dedupShouldEnqueueMulti', () => {
  test('only allows one of several duplicate messages', async () => {
    const opt = getOptionsWithDefaults(Object.assign({ externalDedup: true }, options))
    opt.Redis = Redis
    const message = { MessageBody: 'ls -alh' }
    const messages = [message, message, message, message]
    await expect(dedupShouldEnqueueMulti(messages, opt)).resolves.toEqual([message])
  })
  test('allows multiple unique messages, preventing duplicates', async () => {
    const opt = getOptionsWithDefaults(Object.assign({ fifo: true, externalDedup: true }, options))
    opt.Redis = Redis
    const message1 = addDedupParamsToMessage({ MessageBody: 'ls -alh 1' }, opt)
    const message2 = addDedupParamsToMessage({ MessageBody: 'ls -alh 2' }, opt)
    const message3 = addDedupParamsToMessage({ MessageBody: 'ls -alh 3' }, opt)
    const messages = [message1, message1, message2, message2, message1, message2, message3]
    await expect(dedupShouldEnqueueMulti(messages, opt)).resolves.toEqual([message1, message2, message3])
  })
})
