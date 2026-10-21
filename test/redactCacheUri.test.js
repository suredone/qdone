/* eslint-env jest */

import { redactCacheUri } from '../src/sentry.js'
import * as qdone from '../index.js'

describe('redactCacheUri', () => {
  test('masks the credentials of a cache URI', () => {
    const opt = redactCacheUri({ cacheUri: 'rediss-cluster://:s3cret%40pass@cache.example:6380', other: 1 })
    expect(opt.cacheUri).toBe('rediss-cluster://***@cache.example:6380')
    expect(opt.other).toBe(1)
  })
  test('leaves a URI without credentials and non-objects alone', () => {
    expect(redactCacheUri({ cacheUri: 'redis://cache.example' }).cacheUri).toBe('redis://cache.example')
    expect(redactCacheUri({ cacheTtlSeconds: 10 })).toEqual({ cacheTtlSeconds: 10 })
    expect(redactCacheUri(undefined)).toBeUndefined()
  })
})

describe('package exports', () => {
  test('exposes the shutdown controls embedders rely on', () => {
    expect(typeof qdone.requestShutdown).toBe('function')
    expect(typeof qdone.getShutdownStatus).toBe('function')
    expect(typeof qdone.forceTerminateActiveJobs).toBe('function')
    expect(typeof qdone.processMessages).toBe('function')
  })
  test('forceTerminateActiveJobs reports an empty result before the consumer starts', async () => {
    await expect(qdone.forceTerminateActiveJobs()).resolves.toEqual({ attemptedJobs: 0, failedSignals: [], jobs: [] })
    expect(qdone.getShutdownStatus().phase).toBe('running')
  })
})
