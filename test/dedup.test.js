/* eslint-env jest */
import { jest } from '@jest/globals'
import Redis from 'ioredis-mock'
import { getOptionsWithDefaults } from '../src/defaults.js'
import { shutdownCache, getCacheClient } from '../src/cache.js'
import {
  getCacheKey,
  getDeduplicationId,
  addDedupParamsToMessage,
  dedupShouldEnqueue,
  dedupShouldEnqueueMulti,
  dedupSuccessfullyProcessed,
  dedupSuccessfullyProcessedMulti,
  updateStats,
  statMaintenance
} from '../src/dedup.js'

const options = {
  cacheUri: 'redis://localhost',
  cacheTtlSeconds: 10,
  cachePrefix: 'qdone:'
}

beforeEach(shutdownCache)
afterEach(async () => {
  jest.restoreAllMocks()
  await getCacheClient(options).flushall()
})
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
  test('leaves non-fifo messages untouched when externalDedup is off', () => {
    const opt = getOptionsWithDefaults({ ...options })
    const message = { MessageBody: 'test' }
    expect(addDedupParamsToMessage(message, opt)).toEqual({ ...message })
  })
  test('adds MessageDeduplicationId to fifo messages', () => {
    const opt = getOptionsWithDefaults({ ...options, fifo: true })
    const message = { MessageBody: 'test' }
    expect(addDedupParamsToMessage(message, opt)).toEqual({ ...message, MessageDeduplicationId: 'test' })
  })
  test('uses passed deduplication id when present', () => {
    const opt = getOptionsWithDefaults({ ...options, fifo: true, deduplicationId: 'foo' })
    const message = { MessageBody: 'test' }
    expect(addDedupParamsToMessage(message, opt)).toEqual({ ...message, MessageDeduplicationId: 'foo' })
  })
  test('uses unique deduplication id when dedupIdPerMessage is set', () => {
    const opt = getOptionsWithDefaults({ ...options, fifo: true, dedupIdPerMessage: true })
    opt.uuidFunction = () => 'bar'
    const message = { MessageBody: 'test' }
    expect(addDedupParamsToMessage(message, opt)).toEqual({ ...message, MessageDeduplicationId: 'bar' })
  })
  test('adds fake MessageDeduplicationId to when externalDedup is used on fifo', () => {
    const opt = getOptionsWithDefaults({ ...options, fifo: true, externalDedup: true })
    opt.uuidFunction = () => 'fake-id'
    const message = { MessageBody: 'test' }
    const expected = {
      ...message,
      MessageDeduplicationId: 'fake-id',
      MessageAttributes: {
        QdoneDeduplicationId: {
          DataType: 'String',
          StringValue: 'test'
        }
      }
    }
    expect(addDedupParamsToMessage(message, opt)).toEqual(expected)
  })
  test('still adds MessageAttribute when externalDedup is used on non-fifo', () => {
    const opt = getOptionsWithDefaults({ ...options, externalDedup: true })
    const message = { MessageBody: 'test' }
    const expected = {
      ...message,
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

describe('updateStats', () => {
  test('updates duplicateSet and expirationSet keys', async () => {
    const opt = getOptionsWithDefaults({ ...options })
    const expireAt = new Date().getTime() + opt.dedupPeriod
    const duplicateSet = opt.cachePrefix + 'dedup-stats:duplicateSet'
    const expirationSet = opt.cachePrefix + 'dedup-stats:expirationSet'
    await updateStats('test', 2, expireAt, opt)
    await updateStats('test', 3, expireAt, opt)
    await updateStats('test2', 2, expireAt, opt)
    const client = getCacheClient(opt)
    await expect(
      client.multi()
        .zrange(duplicateSet, '-inf', 'inf', 'BYSCORE', 'WITHSCORES')
        .zrange(expirationSet, '-inf', 'inf', 'BYSCORE', 'WITHSCORES')
        .exec()
    ).resolves.toEqual([
      [null, ['test2', '2', 'test', '3']],
      [null, ['test', expireAt + '', 'test2', expireAt + '']]
    ])
  })
  test('does nothing if duplicates is <1', async () => {
    const opt = getOptionsWithDefaults({ ...options })
    const expireAt = new Date().getTime() + opt.dedupPeriod
    const duplicateSet = opt.cachePrefix + 'dedup-stats:duplicateSet'
    const expirationSet = opt.cachePrefix + 'dedup-stats:expirationSet'
    await updateStats('test', 0, expireAt, opt)
    await updateStats('test', 0, expireAt, opt)
    await updateStats('test2', 0, expireAt, opt)
    const client = getCacheClient(opt)
    await expect(
      client.multi()
        .zrange(duplicateSet, '-inf', 'inf', 'BYSCORE', 'WITHSCORES')
        .zrange(expirationSet, '-inf', 'inf', 'BYSCORE', 'WITHSCORES')
        .exec()
    ).resolves.toEqual([
      [null, []],
      [null, []]
    ])
  })
  test('works with a provided pipeline', async () => {
    const opt = getOptionsWithDefaults({ ...options })
    const expireAt = new Date().getTime() + opt.dedupPeriod
    const duplicateSet = opt.cachePrefix + 'dedup-stats:duplicateSet'
    const expirationSet = opt.cachePrefix + 'dedup-stats:expirationSet'
    const client = getCacheClient(opt)
    const pipeline = client.multi()
    updateStats('test', 2, expireAt, opt, pipeline)
    updateStats('test', 3, expireAt, opt, pipeline)
    updateStats('test2', 2, expireAt, opt, pipeline)
    await pipeline.exec()
    await expect(
      client.multi()
        .zrange(duplicateSet, '-inf', 'inf', 'BYSCORE', 'WITHSCORES')
        .zrange(expirationSet, '-inf', 'inf', 'BYSCORE', 'WITHSCORES')
        .exec()
    ).resolves.toEqual([
      [null, ['test2', '2', 'test', '3']],
      [null, ['test', expireAt + '', 'test2', expireAt + '']]
    ])
  })
})

describe('statMaintenance', () => {
  test('clears out expired keys', async () => {
    const opt = getOptionsWithDefaults({ ...options })
    const duplicateSet = opt.cachePrefix + 'dedup-stats:duplicateSet'
    const expirationSet = opt.cachePrefix + 'dedup-stats:expirationSet'
    const expireAt = new Date().getTime() - 1
    await updateStats('test', 2, expireAt, opt)
    await updateStats('test', 3, expireAt, opt)
    await updateStats('test2', 2, expireAt, opt)
    const client = getCacheClient(opt)
    await statMaintenance(opt)
    await statMaintenance(opt)
    await expect(
      client.multi()
        .zrange(duplicateSet, '-inf', 'inf', 'BYSCORE', 'WITHSCORES')
        .zrange(expirationSet, '-inf', 'inf', 'BYSCORE', 'WITHSCORES')
        .exec()
    ).resolves.toEqual([
      [null, []],
      [null, []]
    ])
  })
})

describe('dedupShouldEnqueue', () => {
  test('resolves true if custom dedup is enabled and cache is empty', async () => {
    const opt = getOptionsWithDefaults(Object.assign({}, options, { externalDedup: true }))
    opt.Redis = Redis
    const message = addDedupParamsToMessage({ MessageBody: 'ls -al' }, opt)
    await expect(dedupShouldEnqueue(message, opt)).resolves.toEqual(true)
  })
  test('resolves false if custom dedup is enabled and cache is full', async () => {
    const opt = getOptionsWithDefaults(Object.assign({}, options, { externalDedup: true }))
    opt.Redis = Redis
    const message = addDedupParamsToMessage({ MessageBody: 'ls -al' }, opt)
    await expect(dedupShouldEnqueue(message, opt)).resolves.toEqual(true)
    await expect(dedupShouldEnqueue(message, opt)).resolves.toEqual(false)
  })
  test('updates stats if dedupStats is set', async () => {
    const opt = getOptionsWithDefaults(Object.assign({}, options, { externalDedup: true, dedupStats: true }))
    const message = addDedupParamsToMessage({ MessageBody: 'ls -al' }, opt)
    await dedupShouldEnqueue(message, opt)
    const expireAt = new Date().getTime() + opt.dedupPeriod
    await dedupShouldEnqueue(message, opt)
    const duplicateSet = opt.cachePrefix + 'dedup-stats:duplicateSet'
    const expirationSet = opt.cachePrefix + 'dedup-stats:expirationSet'
    const client = getCacheClient(opt)
    const expectedKey = opt.cachePrefix + 'dedup:ls_-al'
    await expect(
      client.multi()
        .zrange(duplicateSet, '-inf', 'inf', 'BYSCORE', 'WITHSCORES')
        .zrange(expirationSet, '-inf', 'inf', 'BYSCORE', 'WITHSCORES')
        .exec()
    ).resolves.toEqual([
      [null, [expectedKey, '1']],
      [null, [expectedKey, expireAt + '']]
    ])
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
  test('updates stats if dedupStats is set', async () => {
    const opt = getOptionsWithDefaults(Object.assign({}, options, { externalDedup: true, dedupStats: true }))
    const message1 = addDedupParamsToMessage({ MessageBody: 'ls -alh 1' }, opt)
    const message2 = addDedupParamsToMessage({ MessageBody: 'ls -alh 2' }, opt)
    const message3 = addDedupParamsToMessage({ MessageBody: 'ls -alh 3' }, opt)
    const messages = [message1, message1, message2, message2, message2, message3]
    const expireAt = new Date().getTime() + opt.dedupPeriod
    await dedupShouldEnqueueMulti(messages, opt)
    const duplicateSet = opt.cachePrefix + 'dedup-stats:duplicateSet'
    const expirationSet = opt.cachePrefix + 'dedup-stats:expirationSet'
    const client = getCacheClient(opt)
    const expectedKey1 = opt.cachePrefix + 'dedup:ls_-alh_1'
    const expectedKey2 = opt.cachePrefix + 'dedup:ls_-alh_2'
    await expect(
      client.multi()
        .zrange(duplicateSet, '-inf', 'inf', 'BYSCORE', 'WITHSCORES')
        .zrange(expirationSet, '-inf', 'inf', 'BYSCORE', 'WITHSCORES')
        .exec()
    ).resolves.toEqual([
      [null, [expectedKey1, '1', expectedKey2, '2']],
      [null, [expectedKey1, expireAt + '', expectedKey2, expireAt + '']]
    ])
  })
})

describe('dedupSuccessfullyProcessed', () => {
  test('does nothing if dedup disabled', async () => {
    const opt = getOptionsWithDefaults(options)
    const message = { Body: 'ls -al' }
    await expect(dedupSuccessfullyProcessed(message, opt)).resolves.toBe(0)
  })
  test('shows one key deleted if called after dedupShouldEnqueue', async () => {
    const opt = getOptionsWithDefaults(Object.assign({ externalDedup: true }, options))
    opt.Redis = Redis
    const message = addDedupParamsToMessage({ MessageBody: 'ls -alh' }, opt)
    await dedupShouldEnqueue(message, opt)
    await expect(dedupSuccessfullyProcessed(message, opt)).resolves.toBe(1)
  })
  test('prevents duplicate enqueue but allows subsequent enqueue', async () => {
    const opt = getOptionsWithDefaults(Object.assign({ externalDedup: true }, options))
    opt.Redis = Redis
    const message = addDedupParamsToMessage({ MessageBody: 'ls -alh' }, opt)
    await expect(dedupShouldEnqueue(message, opt)).resolves.toBe(true)
    await expect(dedupShouldEnqueue(message, opt)).resolves.toBe(false)
    await expect(dedupSuccessfullyProcessed(message, opt)).resolves.toBe(1)
    await expect(dedupShouldEnqueue(message, opt)).resolves.toBe(true)
  })
  test('executes stat maintenance if needed', async () => {
    const opt = getOptionsWithDefaults(Object.assign({ externalDedup: true, dedupStats: true }, options))
    const message = addDedupParamsToMessage({ MessageBody: 'ls -alh' }, opt)
    await dedupShouldEnqueue(message, opt)
    const now = new Date()
    const expireAt = now.getTime() + opt.dedupPeriod
    await dedupShouldEnqueue(message, opt)

    // Check stats exist
    const duplicateSet = opt.cachePrefix + 'dedup-stats:duplicateSet'
    const expirationSet = opt.cachePrefix + 'dedup-stats:expirationSet'
    const client = getCacheClient(opt)
    const expectedKey = opt.cachePrefix + 'dedup:ls_-alh'
    await expect(
      client.multi()
        .zrange(duplicateSet, '-inf', 'inf', 'BYSCORE', 'WITHSCORES')
        .zrange(expirationSet, '-inf', 'inf', 'BYSCORE', 'WITHSCORES')
        .exec()
    ).resolves.toEqual([
      [null, [expectedKey, '1']],
      [null, [expectedKey, expireAt + '']]
    ])

    // Fake out the random number geneartor
    jest
      .spyOn(Math, 'random')
      .mockImplementation(() => 0.005)
    await new Promise(resolve => setTimeout(resolve, 1000))
    await expect(dedupSuccessfullyProcessed(message, opt)).resolves.toBe(1)
    jest.restoreAllMocks()

    // Check stats don't exist
    await expect(
      client.multi()
        .zrange(duplicateSet, '-inf', 'inf', 'BYSCORE', 'WITHSCORES')
        .zrange(expirationSet, '-inf', 'inf', 'BYSCORE', 'WITHSCORES')
        .exec()
    ).resolves.toEqual([
      [null, []],
      [null, []]
    ])

    // Cover branch where stat maintenance is not performed
    await dedupSuccessfullyProcessed(message, opt)
  })
})

describe('dedupSuccessfullyProcessedMulti', () => {
  test('does nothing if dedup disabled', async () => {
    const opt = getOptionsWithDefaults(options)
    const message1 = addDedupParamsToMessage({ Body: 'ls -alh 1' }, opt)
    const message2 = addDedupParamsToMessage({ Body: 'ls -alh 2' }, opt)
    const message3 = addDedupParamsToMessage({ Body: 'ls -alh 3' }, opt)
    const messages = [message1, message1, message2, message2, message1, message2, message3]
    await expect(dedupSuccessfullyProcessedMulti(messages, opt)).resolves.toBe(0)
  })
  test('shows the same number of keys deleted after call to dedupShouldEnqueueMulti', async () => {
    const opt = getOptionsWithDefaults(Object.assign({ externalDedup: true }, options))
    opt.Redis = Redis
    const send1 = addDedupParamsToMessage({ MessageBody: 'ls -alh 1' }, opt)
    const send2 = addDedupParamsToMessage({ MessageBody: 'ls -alh 2' }, opt)
    const send3 = addDedupParamsToMessage({ MessageBody: 'ls -alh 3' }, opt)
    const get1 = { ...send1, Body: 'ls -alh 1' }
    const get2 = { ...send2, Body: 'ls -alh 2' }
    const get3 = { ...send3, Body: 'ls -alh 3' }
    const sends = [send1, send2, send3]
    const gets = [get1, get2, get3]
    await dedupShouldEnqueueMulti(sends, opt)
    await expect(dedupSuccessfullyProcessedMulti(gets, opt)).resolves.toEqual(3)
  })
  test('executes stat maintenance if needed', async () => {
    const opt = getOptionsWithDefaults(Object.assign({ externalDedup: true, dedupStats: true }, options))
    const now = new Date()
    const expireAt = now.getTime() + opt.dedupPeriod
    const send1 = addDedupParamsToMessage({ MessageBody: 'ls -alh 1' }, opt)
    const send2 = addDedupParamsToMessage({ MessageBody: 'ls -alh 2' }, opt)
    const send3 = addDedupParamsToMessage({ MessageBody: 'ls -alh 3' }, opt)
    const get1 = { ...send1, Body: 'ls -alh 1' }
    const get2 = { ...send2, Body: 'ls -alh 2' }
    const get3 = { ...send3, Body: 'ls -alh 3' }
    const sends = [send1, send1, send2, send2, send2, send3, send3, send3, send3]
    const gets = [get1, get2, get3]
    await dedupShouldEnqueueMulti(sends, opt)

    // Check stats exist
    const duplicateSet = opt.cachePrefix + 'dedup-stats:duplicateSet'
    const expirationSet = opt.cachePrefix + 'dedup-stats:expirationSet'
    const client = getCacheClient(opt)
    const expectedKey1 = opt.cachePrefix + 'dedup:ls_-alh_1'
    const expectedKey2 = opt.cachePrefix + 'dedup:ls_-alh_2'
    const expectedKey3 = opt.cachePrefix + 'dedup:ls_-alh_3'
    await expect(
      client.multi()
        .zrange(duplicateSet, '-inf', 'inf', 'BYSCORE', 'WITHSCORES')
        .zrange(expirationSet, '-inf', 'inf', 'BYSCORE', 'WITHSCORES')
        .exec()
    ).resolves.toEqual([
      [null, [expectedKey1, '1', expectedKey2, '2', expectedKey3, '3']],
      [null, [expectedKey1, expireAt + '', expectedKey2, expireAt + '', expectedKey3, expireAt + '']]
    ])

    // Fake out the random number geneartor
    jest
      .spyOn(Math, 'random')
      .mockImplementation(() => 2 / 100.0)
    await new Promise(resolve => setTimeout(resolve, 1000))
    await expect(dedupSuccessfullyProcessedMulti(gets, opt)).resolves.toEqual(3)
    jest.restoreAllMocks()

    // Check stats don't exist
    await expect(
      client.multi()
        .zrange(duplicateSet, '-inf', 'inf', 'BYSCORE', 'WITHSCORES')
        .zrange(expirationSet, '-inf', 'inf', 'BYSCORE', 'WITHSCORES')
        .exec()
    ).resolves.toEqual([
      [null, []],
      [null, []]
    ])
    // Cover branch where stats aren't erased
    await dedupSuccessfullyProcessedMulti(gets, opt)
  })
})
