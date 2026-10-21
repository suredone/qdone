/* eslint-env jest */

import { jest } from '@jest/globals'

import { JobExecutor } from '../src/scheduler/jobExecutor.js'
import { setSQSClient } from '../src/sqs.js'

const flushJobs = () => new Promise(resolve => setImmediate(resolve))

function createBatch (groups, fifo = true, failed = []) {
  const started = []
  const gates = new Map()
  const messages = groups.map(([id, group]) => {
    let resolve
    let reject
    const promise = new Promise((_resolve, _reject) => {
      resolve = _resolve
      reject = _reject
    })
    gates.set(id, { promise, resolve, reject })
    return {
      MessageId: id,
      ReceiptHandle: 'receipt-' + id,
      Body: id,
      Attributes: { ApproximateReceiveCount: '1', ...(group === undefined ? {} : { MessageGroupId: group }) }
    }
  })
  const sqsClient = {
    send: jest.fn(async command => ({ Successful: (command.input.Entries || []).map(({ Id }) => ({ Id })) }))
  }
  setSQSClient(sqsClient)
  const executor = new JobExecutor({ prefix: 'fifo-test_', json: false, disableLog: true, killAfter: 3600 })
  const execution = executor.executeJobs(messages, async (_queue, id) => {
    started.push(id)
    await gates.get(id).promise
    if (failed.includes(id)) throw new Error('Expected callback failure')
  }, 'fifo-test_queue', 'https://sqs.us-east-1.amazonaws.com/123/fifo-test_queue' + (fifo ? '.fifo' : ''))
  return {
    started,
    executor,
    execution,
    sqsClient,
    release: id => gates.get(id).resolve(),
    async finish () {
      for (const gate of gates.values()) gate.resolve()
      try {
        await execution
      } finally {
        await executor.shutdown()
      }
    }
  }
}

describe('qdone independent FIFO message groups', () => {
  it('starts independent groups immediately while keeping each group serial', async () => {
    const batch = createBatch([['a1', 'a'], ['a2', 'a'], ['b1', 'b'], ['b2', 'b']])
    try {
      await flushJobs()
      expect(batch.started).toEqual(['a1', 'b1'])
      expect(batch.executor.shutdownStatus()).toMatchObject({ activeJobs: 4, runningJobs: 2, waitingJobs: 2 })
      batch.release('b1')
      await flushJobs()
      expect(batch.started).toEqual(['a1', 'b1', 'b2'])
      batch.release('a1')
      await flushJobs()
      expect(batch.started).toEqual(['a1', 'b1', 'b2', 'a2'])
    } finally {
      await batch.finish()
    }
    expect(batch.executor.activeJobCount()).toBe(0)
  })

  it('preserves group order even when group entries are interleaved', async () => {
    const batch = createBatch([['a1', 'a'], ['b1', 'b'], ['a2', 'a']])
    try {
      await flushJobs()
      expect(batch.started).toEqual(['a1', 'b1'])
      batch.release('a1')
      await flushJobs()
      expect(batch.started).toEqual(['a1', 'b1', 'a2'])
    } finally {
      await batch.finish()
    }
  })

  it('retains parallel execution for a standard queue', async () => {
    const batch = createBatch([['a1', 'a'], ['a2', 'a'], ['a3', 'a']], false)
    try {
      await flushJobs()
      expect(batch.started).toEqual(['a1', 'a2', 'a3'])
    } finally {
      await batch.finish()
    }
  })

  it('conservatively serializes FIFO messages without group attributes', async () => {
    const batch = createBatch([['a1'], ['a2']])
    try {
      await flushJobs()
      expect(batch.started).toEqual(['a1'])
      batch.release('a1')
      await flushJobs()
      expect(batch.started).toEqual(['a1', 'a2'])
    } finally {
      await batch.finish()
    }
  })

  it('keeps callback failure handling and acknowledgement behavior within each group', async () => {
    const batch = createBatch([['a1', 'a'], ['a2', 'a'], ['b1', 'b']], true, ['a1'])
    try {
      await flushJobs()
      expect(batch.started).toEqual(['a1', 'b1'])
      batch.release('a1')
      await flushJobs()
      expect(batch.started).toEqual(['a1', 'b1', 'a2'])
    } finally {
      await batch.finish()
    }
    expect(batch.executor.stats).toMatchObject({ jobsFailed: 1, jobsSucceeded: 2, activeJobs: 0 })
    const deleted = batch.sqsClient.send.mock.calls
      .filter(([command]) => command.constructor.name === 'DeleteMessageBatchCommand')
      .flatMap(([command]) => command.input.Entries.map(entry => entry.Id))
    expect(deleted).toEqual(expect.arrayContaining(['a2', 'b1']))
    expect(deleted).not.toContain('a1')
  })

  it('drains every admitted group and its waiting messages during shutdown', async () => {
    const batch = createBatch([['a1', 'a'], ['a2', 'a'], ['b1', 'b']])
    let shutdown
    try {
      await flushJobs()
      let drained = false
      shutdown = batch.executor.shutdown().then(() => { drained = true })
      batch.release('b1')
      await flushJobs()
      expect(drained).toBe(false)
      batch.release('a1')
      await flushJobs()
      expect(batch.started).toContain('a2')
      expect(drained).toBe(false)
      batch.release('a2')
      await shutdown
      expect(drained).toBe(true)
      expect(batch.executor.shutdownStatus()).toMatchObject({ activeJobs: 0, trackedBatches: 0, trackedJobs: 0 })
    } finally {
      await batch.finish()
      if (shutdown) await shutdown
    }
  })
})
