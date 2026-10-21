/* eslint-env jest */

import { jest } from '@jest/globals'

import os from 'os'

/** @returns {{promise: Promise<unknown>, resolve: Function, reject: Function}} */
function gate () {
  let resolve
  let reject
  const promise = new Promise((_resolve, _reject) => { resolve = _resolve; reject = _reject })
  return { promise, resolve, reject }
}

/** @param {string} id @param {string} group @returns {object} */
function message (id, group = id) {
  return {
    MessageId: id,
    ReceiptHandle: 'receipt-' + id,
    Body: id,
    Attributes: { ApproximateReceiveCount: '1', MessageGroupId: group }
  }
}

describe('qdone admission custody', () => {
  let consumer
  let receives
  let jobs
  let started
  let worker
  let maxJobs
  let resource
  let sqs
  let custodyViolations
  let frames

  beforeEach(async () => {
    jest.resetModules()
    jest.useFakeTimers()
    resource = { free: 1000, load: 0, latency: 1 }
    jest.spyOn(os, 'freemem').mockImplementation(() => resource.free)
    jest.spyOn(os, 'totalmem').mockReturnValue(1000)
    jest.spyOn(os, 'cpus').mockReturnValue([{}, {}, {}, {}])
    const { SystemMonitor } = (await import('../src/scheduler/systemMonitor.js'))
    jest.spyOn(SystemMonitor.prototype, 'measure').mockImplementation(() => {})
    jest.spyOn(SystemMonitor.prototype, 'reportLatency').mockImplementation(() => {})
    jest.spyOn(SystemMonitor.prototype, 'getLatency').mockImplementation(() => resource.latency)
    jest.spyOn(SystemMonitor.prototype, 'getLoad').mockImplementation(() => resource.load)
    consumer = (await import('../src/consumer.js'))
    receives = []
    jobs = new Map()
    started = []
    custodyViolations = []
    frames = []
    jest.spyOn(console, 'error').mockImplementation(frame => {
      if (typeof frame?.maxReturnCount === 'number') frames.push(frame)
    })
    worker = undefined
    sqs = {
      send: jest.fn(async (command, { abortSignal } = {}) => {
        if (command.constructor.name === 'GetQueueUrlCommand') {
          return { QueueUrl: 'https://synthetic.invalid/' + command.input.QueueName }
        }
        if (command.constructor.name === 'ReceiveMessageCommand') {
          const pending = gate()
          const receive = { ...pending, input: command.input, pending: true }
          receives.push(receive)
          const reserved = receives.filter(entry => entry.pending)
            .reduce((sum, entry) => sum + entry.input.MaxNumberOfMessages, 0)
          const accepted = consumer.getShutdownStatus().activeJobs
          if (reserved + accepted > maxJobs) custodyViolations.push({ reserved, accepted, maxJobs })
          abortSignal.addEventListener('abort', () => {
            pending.reject(Object.assign(new Error('Synthetic receive cancellation'), { name: 'AbortError' }))
          }, { once: true })
          try { return await pending.promise } finally { receive.pending = false }
        }
        if (command.constructor.name === 'ChangeMessageVisibilityCommand') return {}
        if (command.constructor.name === 'DeleteMessageBatchCommand') {
          return { Successful: command.input.Entries.map(({ Id }) => ({ Id })) }
        }
        throw new Error('Unexpected queue operation: ' + command.constructor.name)
      }),
      destroy: jest.fn()
    }
    ;(await import('../src/sqs.js')).setSQSClient(sqs)
  })

  /** @param {number} cap @param {string[]} queues @param {object} options @returns {Promise<void>} */
  async function start (cap = 4, queues = ['feed.fifo'], options = {}) {
    maxJobs = cap
    worker = consumer.processMessages(queues, async (_queue, id) => {
      const pending = gate()
      jobs.set(id, pending)
      started.push(id)
      await pending.promise
    }, {
      prefix: 'admission-test_',
      maxConcurrentJobs: cap,
      maxMemoryPercent: 70,
      fifo: true,
      externalDedup: false,
      disableLog: true,
      killAfter: 3600,
      verbose: true,
      ...options
    })
    await jest.advanceTimersByTimeAsync(350)
  }

  /** @param {number} index @param {object[]} messages @returns {Promise<void>} */
  async function deliver (index, messages) {
    expect(messages.length).toBeLessThanOrEqual(receives[index].input.MaxNumberOfMessages)
    receives[index].resolve({ Messages: messages })
    await jest.advanceTimersByTimeAsync(1)
  }

  afterEach(async () => {
    const shutdown = consumer.requestShutdown()
    for (const pending of jobs.values()) pending.resolve()
    // Same-group jobs begin during drain, so release each newly admitted callback too.
    for (let round = 0; round < 10; round++) {
      await jest.advanceTimersByTimeAsync(250)
      for (const pending of jobs.values()) pending.resolve()
    }
    await Promise.all([worker, shutdown])
    jest.useRealTimers()
    jest.restoreAllMocks()
    expect(custodyViolations).toEqual([])
    for (const frame of frames) {
      expect(frame.maxReturnCount).toBeGreaterThanOrEqual(0)
      expect(frame.activeJobs).toBeGreaterThanOrEqual(0)
      expect(frame.allowedJobs).toBe(maxJobs - frame.maxReturnCount - frame.activeJobs)
    }
  })

  it('reuses completed independent siblings while a long callback continues', async () => {
    await start()
    await deliver(0, [message('slow'), message('fast-1'), message('fast-2'), message('fast-3')])
    for (const id of ['fast-1', 'fast-2', 'fast-3']) jobs.get(id).resolve()
    await jest.advanceTimersByTimeAsync(350)
    expect(consumer.getShutdownStatus()).toMatchObject({ activeJobs: 1, runningJobs: 1, waitingJobs: 0 })
    expect(receives).toHaveLength(2)
    expect(receives[1].input.MaxNumberOfMessages).toBe(3)
  })

  it('preserves standard-queue parallelism and reuses completed capacity', async () => {
    await start(4, ['standard'], { fifo: false })
    await deliver(0, ['a1', 'a2', 'a3', 'a4'].map(id => message(id, 'same')))
    expect(started).toEqual(['a1', 'a2', 'a3', 'a4'])
    jobs.get('a2').resolve()
    jobs.get('a3').reject(new Error('Synthetic standard callback rejection'))
    await jest.advanceTimersByTimeAsync(350)
    expect(consumer.getShutdownStatus()).toMatchObject({ activeJobs: 2, runningJobs: 2, waitingJobs: 0 })
    expect(receives[1].input.MaxNumberOfMessages).toBe(2)
  })

  it('counts waiting FIFO jobs and preserves their serial execution', async () => {
    await start()
    await deliver(0, [message('a1', 'a'), message('a2', 'a'), message('a3', 'a')])
    await jest.advanceTimersByTimeAsync(350)
    expect(started).toEqual(['a1'])
    expect(consumer.getShutdownStatus()).toMatchObject({ activeJobs: 3, runningJobs: 1, waitingJobs: 2 })
    expect(receives[1].input.MaxNumberOfMessages).toBe(1)
    await deliver(1, [message('b1', 'b')])
    expect(started).toEqual(['a1', 'b1'])
    jobs.get('a1').resolve()
    await jest.advanceTimersByTimeAsync(1)
    expect(started).toEqual(['a1', 'b1', 'a2'])
  })

  it('applies fractional resource headroom to the newly freed slots', async () => {
    resource.free = 825
    await start()
    expect(receives[0].input.MaxNumberOfMessages).toBe(3)
    await deliver(0, [message('slow'), message('fast-1'), message('fast-2')])
    jobs.get('fast-1').resolve()
    jobs.get('fast-2').resolve()
    await jest.advanceTimersByTimeAsync(350)
    expect(receives[1].input.MaxNumberOfMessages).toBe(2)
  })

  it('retains the bound when a complete receive contains one FIFO group', async () => {
    await start()
    await deliver(0, ['a1', 'a2', 'a3', 'a4'].map(id => message(id, 'a')))
    await jest.advanceTimersByTimeAsync(350)
    expect(started).toEqual(['a1'])
    expect(receives).toHaveLength(1)
    jobs.get('a1').resolve()
    await jest.advanceTimersByTimeAsync(350)
    expect(started).toEqual(['a1', 'a2'])
    expect(receives[1].input.MaxNumberOfMessages).toBe(1)
  })

  it('keeps a newer receive owned when an older batch completes', async () => {
    await start()
    await deliver(0, [message('first')])
    await jest.advanceTimersByTimeAsync(350)
    expect(receives).toHaveLength(2)
    jobs.get('first').resolve()
    await jest.advanceTimersByTimeAsync(700)
    expect(receives).toHaveLength(2)
    expect(receives[1].pending).toBe(true)
  })

  it('rolls back a partial duplicate registration without acknowledging messages', async () => {
    await start()
    const first = message('registered')
    await deliver(0, [first, first])
    expect(started).toEqual([])
    expect(consumer.getShutdownStatus()).toMatchObject({ activeJobs: 0, runningJobs: 0, waitingJobs: 0, trackedJobs: 0 })
    expect(sqs.send.mock.calls.some(([command]) => command.constructor.name === 'DeleteMessageBatchCommand')).toBe(false)
    await jest.advanceTimersByTimeAsync(125000)
    expect(receives[1].input.MaxNumberOfMessages).toBe(4)
    await deliver(1, [first])
    expect(started).toEqual(['registered'])
  })

  it('rolls back malformed JSON before any executor callback or acknowledgement', async () => {
    const { JobExecutor } = (await import('../src/scheduler/jobExecutor.js'))
    const { getOptionsWithDefaults } = (await import('../src/defaults.js'))
    const executor = new JobExecutor({ ...getOptionsWithDefaults({ disableLog: true }), json: true })
    const callback = jest.fn(async () => {})
    const first = { ...message('registered'), Body: '"registered"' }
    await expect(executor.executeJobs([first, message('invalid-json')], callback, 'feed', 'feed.fifo'))
      .rejects.toBeInstanceOf(SyntaxError)
    expect(callback).not.toHaveBeenCalled()
    expect(executor.shutdownStatus()).toMatchObject({ activeJobs: 0, runningJobs: 0, waitingJobs: 0, trackedJobs: 0 })
    expect(sqs.send).not.toHaveBeenCalled()
    await executor.executeJobs([first], callback, 'feed', 'feed.fifo')
    expect(callback).toHaveBeenCalledTimes(1)
    await executor.shutdown()
  })

  it('rolls back accepted registration when a later synchronous side effect throws', async () => {
    const { JobExecutor } = (await import('../src/scheduler/jobExecutor.js'))
    const addJob = JobExecutor.prototype.addJob
    const failure = new Error('Synthetic registration logging failure')
    const registration = jest.spyOn(JobExecutor.prototype, 'addJob').mockImplementation(function (...args) {
      const job = addJob.apply(this, args)
      if (job.message.MessageId === 'throw-after-registration') throw failure
      return job
    })
    const executeJobs = JobExecutor.prototype.executeJobs
    let rejection
    jest.spyOn(JobExecutor.prototype, 'executeJobs').mockImplementation(function (...args) {
      return executeJobs.apply(this, args).catch(error => {
        rejection = error
        throw error
      })
    })
    await start()
    await deliver(0, [message('registered'), message('throw-after-registration')])
    expect(rejection).toBe(failure)
    expect(started).toEqual([])
    expect(consumer.getShutdownStatus()).toMatchObject({ activeJobs: 0, runningJobs: 0, waitingJobs: 0, trackedJobs: 0 })
    expect(sqs.send.mock.calls.some(([command]) => command.constructor.name === 'DeleteMessageBatchCommand')).toBe(false)
    registration.mockRestore()
    await jest.advanceTimersByTimeAsync(125000)
    expect(receives[1].input.MaxNumberOfMessages).toBe(4)
    await deliver(1, [message('registered'), message('throw-after-registration')])
    expect(started).toEqual(['registered', 'throw-after-registration'])
  })

  it('preserves an existing active owner when a later registration duplicates its ID', async () => {
    await start()
    await deliver(0, [message('owner')])
    await jest.advanceTimersByTimeAsync(350)
    await deliver(1, [message('rolled-back'), message('owner')])
    expect(started).toEqual(['owner'])
    expect(consumer.getShutdownStatus()).toMatchObject({ activeJobs: 1, runningJobs: 1, waitingJobs: 0, trackedJobs: 1 })
    jobs.get('owner').resolve()
    await jest.advanceTimersByTimeAsync(125000)
    expect(receives[2].input.MaxNumberOfMessages).toBe(4)
    await deliver(2, [message('rolled-back')])
    expect(started).toEqual(['owner', 'rolled-back'])
  })

  it('counts parallel receives and releases the unused part of a short response', async () => {
    await start(20, ['one.fifo', 'two.fifo'])
    expect(receives).toHaveLength(2)
    await deliver(0, [message('one')])
    await jest.advanceTimersByTimeAsync(350)
    expect(receives).toHaveLength(3)
    expect(receives[2].input.MaxNumberOfMessages).toBe(9)
  })

  it.each(['empty', 'error'])('releases a %s receive reservation', async outcome => {
    await start()
    if (outcome === 'empty') receives[0].resolve({ Messages: [] })
    else receives[0].reject(new Error('Synthetic receive failure'))
    await jest.advanceTimersByTimeAsync(125000)
    expect(receives).toHaveLength(2)
    expect(receives[1].input.MaxNumberOfMessages).toBe(4)
  })

  it.each(['memory', 'load', 'latency'])('preserves %s pause and resume', async kind => {
    if (kind === 'memory') resource.free = 300
    if (kind === 'load') resource.load = 12
    if (kind === 'latency') resource.latency = 100
    await start()
    expect(receives).toHaveLength(0)
    Object.assign(resource, { free: 1000, load: 0, latency: 1 })
    await jest.advanceTimersByTimeAsync(350)
    expect(receives[0].input.MaxNumberOfMessages).toBe(4)
  })

  it('aborts pending receives and drains accepted jobs once on repeated shutdown', async () => {
    await start()
    await deliver(0, [message('one')])
    await jest.advanceTimersByTimeAsync(350)
    const shuttingDown = consumer.requestShutdown()
    const repeated = consumer.requestShutdown()
    jobs.get('one').resolve()
    await jest.advanceTimersByTimeAsync(1500)
    await Promise.all([worker, shuttingDown, repeated])
    expect(sqs.destroy).toHaveBeenCalledTimes(1)
    expect(receives.every(receive => !receive.pending)).toBe(true)
    expect(consumer.getShutdownStatus()).toMatchObject({ activeJobs: 0, runningJobs: 0, waitingJobs: 0 })
  })

  it('releases capacity after a callback rejects without changing its acknowledgement', async () => {
    await start()
    await deliver(0, [message('failed'), message('slow')])
    jobs.get('failed').reject(new Error('Synthetic callback rejection'))
    await jest.advanceTimersByTimeAsync(350)
    expect(receives).toHaveLength(2)
    expect(receives[1].input.MaxNumberOfMessages).toBe(3)
    const deleted = sqs.send.mock.calls.filter(([command]) => command.constructor.name === 'DeleteMessageBatchCommand')
      .flatMap(([command]) => command.input.Entries.map(entry => entry.Id))
    expect(deleted).not.toContain('failed')
  })
})
