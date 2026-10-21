/* eslint-env jest */

import { jest } from '@jest/globals'

import os from 'os'

/** @returns {{promise: Promise<object>, resolve: Function, reject: Function}} */
function deferred () {
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
    Attributes: { MessageGroupId: group, ApproximateReceiveCount: '1' }
  }
}

describe('qdone pending receives during shutdown', () => {
  let consumer
  let receive
  let callback
  let worker
  let sqs
  let aborted
  let deleted
  let callbackGate

  beforeEach(async () => {
    jest.resetModules()
    jest.useFakeTimers()
    jest.spyOn(os, 'freemem').mockReturnValue(1000)
    jest.spyOn(os, 'totalmem').mockReturnValue(1000)
    jest.spyOn(os, 'cpus').mockReturnValue([{}, {}, {}, {}])
    const { SystemMonitor } = (await import('../src/scheduler/systemMonitor.js'))
    jest.spyOn(SystemMonitor.prototype, 'measure').mockImplementation(() => {})
    jest.spyOn(SystemMonitor.prototype, 'reportLatency').mockImplementation(() => {})
    jest.spyOn(SystemMonitor.prototype, 'getLatency').mockReturnValue(1)
    jest.spyOn(SystemMonitor.prototype, 'getLoad').mockReturnValue(0)
    jest.spyOn(console, 'log').mockImplementation(() => {})
    jest.spyOn(console, 'error').mockImplementation(() => {})
    consumer = (await import('../src/consumer.js'))
    receive = deferred()
    callbackGate = deferred()
    callback = jest.fn(async () => { await callbackGate.promise })
    aborted = false
    deleted = []
    worker = undefined
    sqs = {
      send: jest.fn(async (command, { abortSignal } = {}) => {
        switch (command.constructor.name) {
          case 'GetQueueUrlCommand':
            return { QueueUrl: 'https://synthetic.invalid/' + command.input.QueueName }
          case 'ReceiveMessageCommand':
            // Model a response already accepted by SQS: local cancellation cannot undo the receive.
            abortSignal.addEventListener('abort', () => { aborted = true }, { once: true })
            return receive.promise
          case 'ChangeMessageVisibilityCommand': return {}
          case 'DeleteMessageBatchCommand':
            deleted.push(...command.input.Entries.map(({ Id }) => Id))
            return { Successful: command.input.Entries.map(({ Id }) => ({ Id })) }
          default: throw new Error('Unexpected SQS operation: ' + command.constructor.name)
        }
      }),
      destroy: jest.fn()
    }
    ;(await import('../src/sqs.js')).setSQSClient(sqs)
  })

  /** @param {boolean} drainPendingReceives @returns {Promise<void>} */
  async function start (drainPendingReceives) {
    worker = consumer.processMessages(['logs.fifo'], callback, {
      prefix: 'shutdown-test_',
      fifo: true,
      maxConcurrentJobs: 4,
      waitTime: 20,
      killAfter: 3600,
      externalDedup: false,
      disableLog: true,
      drainPendingReceives
    })
    await jest.advanceTimersByTimeAsync(350)
    expect(consumer.getShutdownStatus().receives).toBe(1)
  }

  afterEach(async () => {
    const shutdown = consumer.requestShutdown()
    receive.resolve({ Messages: [] })
    callbackGate.resolve()
    await jest.advanceTimersByTimeAsync(1500)
    await Promise.all([worker, shutdown])
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  it('drains and deletes a response delivered after shutdown instead of abandoning its messages', async () => {
    await start(true)
    let finished = false
    const shutdown = consumer.requestShutdown().then(() => { finished = true })
    await jest.advanceTimersByTimeAsync(1)
    expect(aborted).toBe(false)
    expect(finished).toBe(false)
    receive.resolve({ Messages: [message('accepted')] })
    await jest.advanceTimersByTimeAsync(1)
    expect(callback).toHaveBeenCalledTimes(1)
    expect(callback.mock.calls[0][1]).toBe('accepted')
    expect(finished).toBe(false)
    expect(deleted).toEqual([])
    expect(sqs.destroy).not.toHaveBeenCalled()
    callbackGate.resolve()
    await jest.advanceTimersByTimeAsync(1500)
    await shutdown
    expect(deleted).toEqual(['accepted'])
    expect(finished).toBe(true)
    expect(sqs.send.mock.calls.filter(([command]) => command.constructor.name === 'ReceiveMessageCommand'))
      .toHaveLength(1)
  })

  it('preserves FIFO ordering and leaves failed work unacknowledged during the final receive drain', async () => {
    callback.mockImplementation(async (_queue, id) => {
      if (id === 'failed') {
        await callbackGate.promise
        throw new Error('Synthetic job failure')
      }
    })
    await start(true)
    const shutdown = consumer.requestShutdown()
    receive.resolve({ Messages: [message('failed', 'a'), message('successor', 'a'), message('independent', 'b')] })
    await jest.advanceTimersByTimeAsync(1)
    expect(callback.mock.calls.map(args => args[1])).toEqual(['failed', 'independent'])
    callbackGate.resolve()
    await jest.advanceTimersByTimeAsync(1500)
    await shutdown
    expect(callback.mock.calls.map(args => args[1])).toEqual(['failed', 'independent', 'successor'])
    expect(deleted.sort()).toEqual(['independent', 'successor'])
  })

  it('waits for an empty twenty-second poll without opening another receive', async () => {
    await start(true)
    let finished = false
    const shutdown = consumer.requestShutdown().then(() => { finished = true })
    await jest.advanceTimersByTimeAsync(20000)
    expect(aborted).toBe(false)
    expect(finished).toBe(false)
    receive.resolve({ Messages: [] })
    await jest.advanceTimersByTimeAsync(1)
    await shutdown
    expect(callback).not.toHaveBeenCalled()
    expect(deleted).toEqual([])
    expect(sqs.send.mock.calls.filter(([command]) => command.constructor.name === 'ReceiveMessageCommand'))
      .toHaveLength(1)
  })

  it('releases a failed receive during shutdown without starting work or another receive', async () => {
    await start(true)
    const shutdown = consumer.requestShutdown()
    receive.reject(new Error('Synthetic receive failure'))
    await jest.advanceTimersByTimeAsync(1)
    await shutdown
    expect(consumer.getShutdownStatus()).toMatchObject({ phase: 'complete', receives: 0, activeJobs: 0 })
    expect(callback).not.toHaveBeenCalled()
    expect(deleted).toEqual([])
    expect(sqs.send.mock.calls.filter(([command]) => command.constructor.name === 'ReceiveMessageCommand'))
      .toHaveLength(1)
  })

  it.each([false, undefined])('retains cancellation without explicit opt-in (%s)', async optIn => {
    await start(optIn)
    const shutdown = consumer.requestShutdown()
    await jest.advanceTimersByTimeAsync(1)
    expect(aborted).toBe(true)
    receive.reject(Object.assign(new Error('Synthetic canceled poll'), { name: 'AbortError' }))
    await jest.advanceTimersByTimeAsync(1)
    await shutdown
    expect(callback).not.toHaveBeenCalled()
    expect(deleted).toEqual([])
  })
})
