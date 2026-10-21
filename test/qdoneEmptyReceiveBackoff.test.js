/* eslint-env jest */

import { jest } from '@jest/globals'

import os from 'os'

describe('qdone empty-receive cooldown', () => {
  let consumer
  let worker
  let receives
  let completed
  let firstResponse
  let firstSeen
  let maxPending
  let pending
  let attributesActive

  beforeEach(async () => {
    jest.resetModules()
    jest.useFakeTimers()
    jest.spyOn(Math, 'random').mockReturnValue(0.5)
    jest.spyOn(os, 'freemem').mockReturnValue(1000)
    jest.spyOn(os, 'totalmem').mockReturnValue(1000)
    jest.spyOn(os, 'cpus').mockReturnValue([{}, {}, {}, {}])
    const { SystemMonitor } = (await import('../src/scheduler/systemMonitor.js'))
    jest.spyOn(SystemMonitor.prototype, 'measure').mockImplementation(() => {})
    jest.spyOn(SystemMonitor.prototype, 'reportLatency').mockImplementation(() => {})
    jest.spyOn(SystemMonitor.prototype, 'getLatency').mockReturnValue(1)
    jest.spyOn(SystemMonitor.prototype, 'getLoad').mockReturnValue(0)
    consumer = (await import('../src/consumer.js'))
    receives = []
    completed = []
    firstSeen = new Set()
    firstResponse = 'empty'
    attributesActive = true
    pending = 0
    maxPending = 0
    worker = undefined
    const sqs = {
      send: jest.fn(async (command, { abortSignal } = {}) => {
        const name = command.constructor.name
        if (name === 'GetQueueUrlCommand') return { QueueUrl: 'https://synthetic.invalid/' + command.input.QueueName }
        if (name === 'GetQueueAttributesCommand') {
          return { Attributes: { ApproximateNumberOfMessages: attributesActive ? '1' : '0', ApproximateNumberOfMessagesNotVisible: '0', ApproximateNumberOfMessagesDelayed: '0' } }
        }
        if (name === 'ReceiveMessageCommand') {
          const qrl = command.input.QueueUrl
          receives.push({ qrl, at: Date.now(), max: command.input.MaxNumberOfMessages })
          if (!firstSeen.has(qrl)) {
            firstSeen.add(qrl)
            if (qrl.endsWith('ready')) {
              return { Messages: [{ MessageId: 'ready', ReceiptHandle: 'r', Body: '{}', Attributes: {} }] }
            }
            if (firstResponse === 'error') throw new Error('Synthetic receive error')
            return { Messages: [] }
          }
          pending++
          maxPending = Math.max(maxPending, pending)
          try {
            return await new Promise((_resolve, reject) => {
              abortSignal.addEventListener('abort', () => reject(Object.assign(new Error('abort'), { name: 'AbortError' })), { once: true })
            })
          } finally { pending-- }
        }
        if (name === 'DeleteMessageBatchCommand') return { Successful: command.input.Entries.map(({ Id }) => ({ Id })) }
        if (name === 'ChangeMessageVisibilityCommand') return {}
        throw new Error('Unexpected SQS call ' + name)
      }),
      destroy: jest.fn()
    }
    ;(await import('../src/sqs.js')).setSQSClient(sqs)
  })

  /** @param {object} options @param {string[]} queues @returns {Promise<void>} */
  async function start (options = {}, queues = ['empty']) {
    worker = consumer.processMessages(queues, async (queue) => { completed.push(queue) }, {
      prefix: 'cooldown_',
      maxConcurrentJobs: 2,
      batchLimit: 1,
      disableLog: true,
      killAfter: 3600,
      ...options
    })
    await jest.advanceTimersByTimeAsync(1000)
  }

  afterEach(async () => {
    if (worker) {
      const shutdown = consumer.requestShutdown()
      await jest.advanceTimersByTimeAsync(1000)
      await Promise.all([worker, shutdown])
    }
    expect(maxPending).toBeLessThanOrEqual(2)
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  it('resumes a successfully empty queue by the next discovery with the cap', async () => {
    await start({ emptyReceiveBackoffSeconds: 5 })
    expect(receives).toHaveLength(1)
    await jest.advanceTimersByTimeAsync(11000)
    expect(receives).toHaveLength(2)
    expect(receives[1].at - receives[0].at).toBeLessThanOrEqual(12000)
  })

  it('resumes through the production active-only and ten-second cache path', async () => {
    const Redis = (await import('ioredis-mock')).default
    await start({ emptyReceiveBackoffSeconds: 5, activeOnly: true, cacheUri: 'redis://synthetic.invalid', Redis, cacheTtlSeconds: 10 })
    expect(receives).toHaveLength(1)
    await jest.advanceTimersByTimeAsync(11000)
    expect(receives).toHaveLength(2)
  })

  it('excludes idle queues until active-only discovery reports them active', async () => {
    attributesActive = false
    await start({ emptyReceiveBackoffSeconds: 5, activeOnly: true })
    await jest.advanceTimersByTimeAsync(11000)
    expect(receives).toHaveLength(0)
    attributesActive = true
    await jest.advanceTimersByTimeAsync(10000)
    expect(receives).toHaveLength(1)
  })

  it('preserves the default empty-response backoff when not opted in', async () => {
    await start()
    await jest.advanceTimersByTimeAsync(18000)
    expect(receives).toHaveLength(1)
    await jest.advanceTimersByTimeAsync(12000)
    expect(receives).toHaveLength(2)
    expect(receives[1].at - receives[0].at).toBeGreaterThan(20000)
  })

  it('preserves error backoff even when the empty-response cap is set', async () => {
    firstResponse = 'error'
    await start({ emptyReceiveBackoffSeconds: 5 })
    await jest.advanceTimersByTimeAsync(18000)
    expect(receives).toHaveLength(1)
    await jest.advanceTimersByTimeAsync(12000)
    expect(receives).toHaveLength(2)
    expect(receives[1].at - receives[0].at).toBeGreaterThan(20000)
  })

  it('keeps another ready queue progressing while the empty queue cools down', async () => {
    await start({ emptyReceiveBackoffSeconds: 5 }, ['empty', 'ready'])
    expect(completed).toEqual(['ready'])
    await jest.advanceTimersByTimeAsync(11000)
    expect(receives.filter(row => row.qrl.endsWith('empty'))).toHaveLength(2)
    expect(completed).toHaveLength(1)
  })

  it.each([0, -1, 121, 1.5, NaN, Infinity, true, null, '5'])('rejects invalid API cap %s', async value => {
    const { getOptionsWithDefaults } = (await import('../src/defaults.js'))
    expect(() => getOptionsWithDefaults({ emptyReceiveBackoffSeconds: value })).toThrow('integer between 1 and 120')
  })

  it('keeps error escalation and successful reset intact', async () => {
    const { QueueManager } = (await import('../src/scheduler/queueManager.js'))
    const manager = Object.create(QueueManager.prototype)
    manager.opt = { emptyReceiveBackoffSeconds: 5 }
    manager.icehouse = {}
    manager.updateIcehouse('queue', true, true)
    expect(manager.icehouse.queue.secondsToWait).toBe(20)
    manager.updateIcehouse('queue', true, true)
    expect(manager.icehouse.queue.secondsToWait).toBe(80)
    manager.updateIcehouse('queue', true, true)
    expect(manager.icehouse.queue.secondsToWait).toBe(120)
    manager.updateIcehouse('queue', false)
    expect(manager.icehouse.queue).toBeUndefined()
  })
})
