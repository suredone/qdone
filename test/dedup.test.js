/* eslint-env jest */

import Redis from 'ioredis-mock'
import { getOptionsWithDefaults } from '../src/defaults.js'
import { shutdownCache, getCacheClient } from '../src/cache.js'
import {
  getCacheKey,
  dedupShouldEnqueue,
  dedupShouldEnqueueMulti,
  dedupSuccessfullyProcessed,
  dedupSuccessfullyProcessedMulti
} from '../src/dedup.js'

const options = {
  cacheUri: 'redis://localhost',
  cacheTtlSeconds: 10,
  cachePrefix: 'qdone:'
}

beforeEach(shutdownCache)
afterEach(async () => getCacheClient(options).flushall())
afterAll(shutdownCache)

describe('getCacheKey', () => {
  test('creates key based on body when dedup id not specified', () => {
    const opt = getOptionsWithDefaults(options)
    opt.Redis = new Redis({ data: {} })
    const message = { MessageBody: 'ls -al' }
    expect(getCacheKey(message, opt)).toEqual('qdone:dedup:ls -al')
  })
  test('creates key based on dedup id when present', () => {
    const opt = getOptionsWithDefaults(options)
    opt.Redis = new Redis({ data: {} })
    const message = {
      MessageDeduplicationId: 'test',
      MessageBody: 'ls -al'
    }
    expect(getCacheKey(message, opt)).toEqual('qdone:dedup:test')
  })
  test('creates key based on sha1 when body is too long', () => {
    const opt = getOptionsWithDefaults(options)
    opt.Redis = new Redis({ data: {} })
    const message = { MessageBody: 'asdf '.repeat(1000) }
    expect(getCacheKey(message, opt)).toEqual('qdone:dedup:asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asdf asd...sha1:55c3f60a6c8ae4296dcb88aa7daaf8090d7622c6')
  })
  test('ignores whitespace', () => {
    const opt = getOptionsWithDefaults(options)
    const message = { MessageBody: ' ls -al    \t' }
    expect(getCacheKey(message, opt)).toEqual('qdone:dedup:ls -al')
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
    const opt = getOptionsWithDefaults(Object.assign({}, options, { dedupMethod: 'redis' }))
    opt.Redis = Redis
    const message = { MessageBody: 'ls -al' }
    await expect(dedupShouldEnqueue(message, opt)).resolves.toEqual(true)
  })
  test('resolves false if custom dedup is enabled and cache is full', async () => {
    const opt = getOptionsWithDefaults(Object.assign({}, options, { dedupMethod: 'redis' }))
    opt.Redis = Redis
    const message = { MessageBody: 'ls -al' }
    await expect(dedupShouldEnqueue(message, opt)).resolves.toEqual(true)
    await expect(dedupShouldEnqueue(message, opt)).resolves.toEqual(false)
  })
})

describe('dedupShouldEnqueueMulti', () => {
  test('leaves messages untouched if dedup is not enabled', async () => {
    const opt = getOptionsWithDefaults(options)
    opt.Redis = Redis
    const message = { MessageBody: 'ls -al' }
    const messages = [message, message, message]
    await expect(dedupShouldEnqueueMulti(messages, opt)).resolves.toBe(messages)
  })
  test('only allows one of several duplicate messages', async () => {
    const opt = getOptionsWithDefaults(Object.assign({ dedupMethod: 'redis' }, options))
    opt.Redis = Redis
    const message = { MessageBody: 'ls -alh' }
    const messages = [message, message, message, message]
    await expect(dedupShouldEnqueueMulti(messages, opt)).resolves.toEqual([message])
  })
  test('allows multiple unique messages, preventing duplicates', async () => {
    const opt = getOptionsWithDefaults(Object.assign({ dedupMethod: 'redis' }, options))
    opt.Redis = Redis
    const message1 = { MessageBody: 'ls -alh 1' }
    const message2 = { MessageBody: 'ls -alh 2' }
    const message3 = { MessageBody: 'ls -alh 3' }
    const messages = [message1, message1, message2, message2, message1, message2, message3]
    await expect(dedupShouldEnqueueMulti(messages, opt)).resolves.toEqual([message1, message2, message3])
  })
})

describe('dedupSuccessfullyProcessed', () => {
  test('does nothing if dedup disabled', async () => {
    const opt = getOptionsWithDefaults(options)
    const message = { Body: 'ls -al' }
    await expect(dedupSuccessfullyProcessed(message, opt)).resolves.toBe(0)
  })
  test('shows one key deleted if called after dedupShouldEnqueue', async () => {
    const opt = getOptionsWithDefaults(Object.assign({ dedupMethod: 'redis' }, options))
    opt.Redis = Redis
    const message = { MessageBody: 'ls -alh' }
    await dedupShouldEnqueue(message, opt)
    await expect(dedupSuccessfullyProcessed(message, opt)).resolves.toBe(1)
  })
  test('prevents duplicate enqueue but allows subsequent enqueue', async () => {
    const opt = getOptionsWithDefaults(Object.assign({ dedupMethod: 'redis' }, options))
    opt.Redis = Redis
    const message = { MessageBody: 'ls -alh' }
    await expect(dedupShouldEnqueue(message, opt)).resolves.toBe(true)
    await expect(dedupShouldEnqueue(message, opt)).resolves.toBe(false)
    await expect(dedupSuccessfullyProcessed(message, opt)).resolves.toBe(1)
    await expect(dedupShouldEnqueue(message, opt)).resolves.toBe(true)
  })
})

describe('dedupSuccessfullyProcessedMulti', () => {
  test('does nothing if dedup disabled', async () => {
    const opt = getOptionsWithDefaults(options)
    const message1 = { Body: 'ls -alh 1' }
    const message2 = { Body: 'ls -alh 2' }
    const message3 = { Body: 'ls -alh 3' }
    const messages = [message1, message1, message2, message2, message1, message2, message3]
    await expect(dedupSuccessfullyProcessedMulti(messages, opt)).resolves.toBe(0)
  })
  test('shows the same number of keys deleted after call to dedupShouldEnqueueMulti', async () => {
    const opt = getOptionsWithDefaults(Object.assign({ dedupMethod: 'redis' }, options))
    opt.Redis = Redis
    const send1 = { MessageBody: 'ls -alh 1' }
    const send2 = { MessageBody: 'ls -alh 2' }
    const send3 = { MessageBody: 'ls -alh 3' }
    const get1 = { Body: 'ls -alh 1' }
    const get2 = { Body: 'ls -alh 2' }
    const get3 = { Body: 'ls -alh 3' }
    const sends = [send1, send2, send3]
    const gets = [get1, get2, get3]
    await dedupShouldEnqueueMulti(sends, opt)
    await expect(dedupSuccessfullyProcessedMulti(gets, opt)).resolves.toEqual(3)
  })
})
