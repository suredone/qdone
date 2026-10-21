/* eslint-env jest */

import { jest } from '@jest/globals'

/** @returns {{promise: Promise<unknown>, resolve: Function, reject: Function}} */
function gate () {
  let resolve
  let reject
  const promise = new Promise((_resolve, _reject) => { resolve = _resolve; reject = _reject })
  return { promise, resolve, reject }
}

/** @param {string} id @param {{group?: string, dedupId?: string}} options @returns {object} */
function message (id, { group = id, dedupId } = {}) {
  return {
    MessageId: id,
    ReceiptHandle: 'receipt-' + id,
    Body: id,
    Attributes: { ApproximateReceiveCount: '1', MessageGroupId: group },
    ...(dedupId ? { MessageAttributes: { QdoneDeduplicationId: { DataType: 'String', StringValue: dedupId } } } : {})
  }
}

/** A mock SQS client whose DeleteMessageBatch calls can be held open one at a time. */
function sqsMock () {
  const calls = []
  const holds = []
  return {
    calls,
    holdNextDelete () {
      const held = gate()
      holds.push(held)
      return held
    },
    send: jest.fn(async command => {
      const name = command.constructor.name
      calls.push({ name, input: command.input })
      if (name === 'DeleteMessageBatchCommand' && holds.length) await holds.shift().promise
      if (command.input.Entries) return { Successful: command.input.Entries.map(({ Id }) => ({ Id })) }
      return {}
    }),
    destroy: jest.fn()
  }
}

const deleteCalls = client => client.calls.filter(call => call.name === 'DeleteMessageBatchCommand')
const deletedIds = client => deleteCalls(client).flatMap(call => call.input.Entries.map(entry => entry.Id))
const extendedIds = client => client.calls
  .filter(call => call.name === 'ChangeMessageVisibilityBatchCommand')
  .flatMap(call => call.input.Entries.map(entry => entry.Id))

const FIFO_QRL = 'https://sqs.us-east-1.amazonaws.com/123/prompt-test_queue.fifo'
const STANDARD_QRL = 'https://sqs.us-east-1.amazonaws.com/123/prompt-test_queue'

describe('qdone prompt FIFO delete', () => {
  let JobExecutor
  let sqsModule
  let sqs
  let redis
  let executor

  beforeEach(async () => {
    jest.resetModules()
    jest.useFakeTimers()
    ;({ JobExecutor } = (await import('../src/scheduler/jobExecutor.js')))
    sqsModule = (await import('../src/sqs.js'))
    sqs = sqsMock()
    sqsModule.setSQSClient(sqs)
    redis = { del: jest.fn(async () => 1), quit: jest.fn(async () => 'OK') }
  })

  afterEach(async () => {
    jest.clearAllTimers()
    jest.useRealTimers()
  })

  /** @param {object} options @returns {object} */
  function createExecutor (options = {}) {
    executor = new JobExecutor({
      prefix: 'prompt-test_',
      json: false,
      disableLog: true,
      killAfter: 3600,
      cacheUri: 'redis://synthetic.invalid:6379',
      cachePrefix: 'test:',
      Redis: function FakeRedis () { return redis },
      ...options
    })
    return executor
  }

  /**
   * Starts one job whose callback waits on a gate, and returns handles to finish it.
   * @param {object} msg @param {string} qrl @returns {object}
   */
  function startJob (msg, qrl = FIFO_QRL, { fail = false, onStart } = {}) {
    const release = gate()
    const execution = executor.executeJobs([msg], async (_queue, _payload, attributes) => {
      onStart?.(attributes)
      await release.promise
      if (fail) throw new Error('Expected callback failure')
    }, 'prompt-test_queue', qrl)
    return {
      execution,
      async finish () {
        release.resolve()
        await execution
      }
    }
  }

  it('deletes a completed FIFO message on the next timer tick instead of the next 10s pass', async () => {
    createExecutor()
    const job = startJob(message('a1'))
    await job.finish()

    await jest.advanceTimersByTimeAsync(1)

    expect(deletedIds(sqs)).toEqual(['a1'])
    expect(executor.shutdownStatus()).toMatchObject({ trackedJobs: 0, activeJobs: 0 })
    await executor.shutdown()
  })

  it('leaves a completed standard-queue message to the 10s pass', async () => {
    createExecutor()
    const job = startJob(message('s1'), STANDARD_QRL)
    await job.finish()

    await jest.advanceTimersByTimeAsync(1)
    expect(deletedIds(sqs)).toEqual([])

    await jest.advanceTimersByTimeAsync(10 * 1000)
    expect(deletedIds(sqs)).toEqual(['s1'])
    await executor.shutdown()
  })

  it('never deletes a failed, killed or force-terminated FIFO job', async () => {
    const killTree = jest.fn((_pid, _signal, callback) => callback?.())
    createExecutor({ killAfter: 1, killTree, forceKillDelayMs: 0 })

    const failed = startJob(message('failed'), FIFO_QRL, { fail: true })
    await failed.finish()
    await jest.advanceTimersByTimeAsync(1)

    const killed = startJob(message('killed'), FIFO_QRL, { onStart: attributes => attributes.registerPid(4321) })
    await jest.advanceTimersByTimeAsync(1000)
    expect(killTree).toHaveBeenCalledWith(4321, 'SIGTERM', expect.any(Function))
    await killed.finish()
    await jest.advanceTimersByTimeAsync(1)

    const forced = startJob(message('forced'), FIFO_QRL, { onStart: attributes => attributes.registerPid(4322) })
    await jest.advanceTimersByTimeAsync(0)
    const forcing = executor.forceTerminateActiveJobs()
    await jest.advanceTimersByTimeAsync(10)
    await expect(forcing).resolves.toMatchObject({ attemptedJobs: 1 })
    await forced.finish()
    await jest.advanceTimersByTimeAsync(1)

    await startJob(message('succeeded')).finish()
    await jest.advanceTimersByTimeAsync(30 * 1000)

    expect(deletedIds(sqs)).toEqual(['succeeded'])
    expect(executor.stats).toMatchObject({ jobsFailed: 3, jobsSucceeded: 1, activeJobs: 0 })
    expect(executor.shutdownStatus().trackedJobs).toBe(0)
    await executor.shutdown()
  })

  it('coalesces completions in the same tick into one pass with full batches', async () => {
    createExecutor()
    const passes = jest.spyOn(executor, 'maintainVisibilityPass')
    const messages = Array.from({ length: 25 }, (_value, index) => message('m' + index))
    await executor.executeJobs(messages, async () => {}, 'prompt-test_queue', FIFO_QRL)

    await jest.advanceTimersByTimeAsync(1)

    expect(passes).toHaveBeenCalledTimes(1)
    expect(deleteCalls(sqs).map(call => call.input.Entries.length)).toEqual([10, 10, 5])
    await executor.shutdown()
  })

  it('marks a slow delete batch processed even after a later pass untracked its jobs', async () => {
    createExecutor()
    const heldDelete = sqs.holdNextDelete()
    const first = startJob(message('x1', { dedupId: 'dedup-x1' }))
    await first.finish()
    await jest.advanceTimersByTimeAsync(10 * 1000)
    expect(deletedIds(sqs)).toEqual(['x1'])

    // A later pass deletes y1 and runs its cleanup while x1's batch is still in flight.
    const second = startJob(message('y1', { dedupId: 'dedup-y1' }))
    await second.finish()
    await jest.advanceTimersByTimeAsync(10 * 1000)
    expect(deletedIds(sqs)).toEqual(['x1', 'y1'])
    expect(executor.shutdownStatus().trackedJobs).toBe(0)

    heldDelete.resolve()
    await jest.advanceTimersByTimeAsync(0)

    expect(redis.del.mock.calls.map(([key]) => key).sort()).toEqual(['test:dedup:dedup-x1', 'test:dedup:dedup-y1'])
    await executor.shutdown()
  })

  it('does not finish shutdown while an earlier pass still has a delete in flight', async () => {
    createExecutor()
    const heldDelete = sqs.holdNextDelete()
    const first = startJob(message('x1'))
    await first.finish()
    await jest.advanceTimersByTimeAsync(10 * 1000)
    const second = startJob(message('y1'))
    await second.finish()
    await jest.advanceTimersByTimeAsync(10 * 1000)
    expect(deletedIds(sqs)).toEqual(['x1', 'y1'])

    let shutdownResolved = false
    const shutdown = executor.shutdown().then(() => { shutdownResolved = true })
    await jest.advanceTimersByTimeAsync(5 * 1000)
    expect(shutdownResolved).toBe(false)

    heldDelete.resolve()
    await shutdown
    const callsAtShutdown = sqs.calls.length
    await jest.advanceTimersByTimeAsync(30 * 1000)
    expect(sqs.calls).toHaveLength(callsAtShutdown)
  })

  it('keeps extending running jobs while an earlier pass is stuck on a dedup delete', async () => {
    createExecutor()
    const stuck = gate()
    redis.del.mockImplementationOnce(() => stuck.promise)
    const longJob = startJob(message('long'))
    const done = startJob(message('x1', { dedupId: 'dedup-x1' }))
    await done.finish()
    await jest.advanceTimersByTimeAsync(10 * 1000)
    expect(redis.del).toHaveBeenCalledTimes(1)

    // The long job's first extension is due at half its 120s receive visibility.
    await jest.advanceTimersByTimeAsync(60 * 1000)
    expect(extendedIds(sqs)).toContain('long')

    stuck.resolve(1)
    await longJob.finish()
    await jest.advanceTimersByTimeAsync(1)
    await executor.shutdown()
  })

  it('stops starting early passes at the cap but keeps the 10s pass deleting and extending', async () => {
    createExecutor()
    const passes = jest.spyOn(executor, 'maintainVisibilityPass')
    const holds = [sqs.holdNextDelete(), sqs.holdNextDelete(), sqs.holdNextDelete()]
    const longJob = startJob(message('long'))
    for (const id of ['x1', 'x2', 'x3']) {
      await startJob(message(id)).finish()
      await jest.advanceTimersByTimeAsync(1)
    }
    expect(deletedIds(sqs)).toEqual(['x1', 'x2', 'x3'])
    expect(executor.shutdownStatus().maintenancePasses).toBe(3)
    const passesAtCap = passes.mock.calls.length

    await startJob(message('x4')).finish()
    await jest.advanceTimersByTimeAsync(1)
    expect(passes).toHaveBeenCalledTimes(passesAtCap)
    expect(deletedIds(sqs)).not.toContain('x4')

    await jest.advanceTimersByTimeAsync(10 * 1000)
    expect(deletedIds(sqs)).toContain('x4')
    await jest.advanceTimersByTimeAsync(51 * 1000)
    expect(extendedIds(sqs)).toContain('long')

    for (const held of holds) held.resolve()
    await longJob.finish()
    await jest.advanceTimersByTimeAsync(1)
    await executor.shutdown()
  })

  // Jest fails a test on any unhandled rejection, so this also proves shutdown owns the failed pass's
  // rejection instead of reporting it a second time.
  it('finishes every pass and the final pass before rejecting shutdown for a failed pass', async () => {
    createExecutor()
    const passes = jest.spyOn(executor, 'maintainVisibilityPass')
    const redisError = new Error('synthetic redis failure')
    const failingDel = gate()
    redis.del.mockImplementationOnce(() => failingDel.promise)
    await startJob(message('x1', { dedupId: 'dedup-x1' })).finish()
    await jest.advanceTimersByTimeAsync(10 * 1000)

    const heldDelete = sqs.holdNextDelete()
    await startJob(message('y1')).finish()
    await jest.advanceTimersByTimeAsync(10 * 1000)
    expect(deletedIds(sqs)).toEqual(['x1', 'y1'])

    let settled = false
    const shutdown = executor.shutdown().finally(() => { settled = true })
    shutdown.catch(() => {})
    await jest.advanceTimersByTimeAsync(5 * 1000)
    expect(settled).toBe(false)

    // The first pass fails while the second still holds a delete open.
    failingDel.reject(redisError)
    await jest.advanceTimersByTimeAsync(5 * 1000)
    expect(settled).toBe(false)
    const passesBeforeRelease = passes.mock.calls.length

    heldDelete.resolve()
    await expect(shutdown).rejects.toMatchObject({ errors: [redisError] })
    expect(passes.mock.calls.length).toBeGreaterThan(passesBeforeRelease)
    expect(executor.shutdownStatus()).toMatchObject({ trackedJobs: 0, maintenancePasses: 0 })
  })

  it('reports why the final pass failed alongside the shutdown invariant failure', async () => {
    createExecutor()
    const redisError = new Error('synthetic redis failure')
    redis.del.mockImplementationOnce(async () => { throw redisError })
    const job = startJob(message('b1', { dedupId: 'dedup-b1' }))
    await jest.advanceTimersByTimeAsync(0)

    // Shutdown starts while b1 runs, so the final pass is its only delete, and that pass fails
    // on the dedup mark before its cleanup untracks b1.
    const shutdown = executor.shutdown().then(() => undefined, reason => reason)
    await job.finish()
    const error = await shutdown

    expect(deletedIds(sqs)).toEqual(['b1'])
    expect(error?.errors).toEqual([
      expect.objectContaining({ message: expect.stringMatching(/^jobExecutor shutdown invariant failed/) }),
      redisError
    ])
  })

  it('sends deletes and extensions on the maintenance client and receives on the main client', async () => {
    const main = sqsMock()
    const maintenance = sqsMock()
    sqsModule.setSQSClient(main, maintenance)
    createExecutor({ killAfter: 60 })
    const { getMessages } = (await import('../src/consumer.js'))

    const longJob = startJob(message('long'), FIFO_QRL, { onStart: attributes => attributes.registerPid(4321) })
    await startJob(message('x1')).finish()
    await jest.advanceTimersByTimeAsync(31 * 1000)
    await getMessages(FIFO_QRL, { waitTime: 1 }, 1)
    await sqsModule.getQueueAttributes([FIFO_QRL])

    const names = client => [...new Set(client.calls.map(call => call.name))].sort()
    expect(names(maintenance)).toEqual([
      'ChangeMessageVisibilityBatchCommand',
      'ChangeMessageVisibilityCommand',
      'DeleteMessageBatchCommand'
    ])
    expect(names(main)).toEqual(['GetQueueAttributesCommand', 'ReceiveMessageCommand'])

    await longJob.finish()
    await jest.advanceTimersByTimeAsync(1)
    await executor.shutdown()
    await sqsModule.shutdownSQSClient()
    expect(main.destroy).toHaveBeenCalledTimes(1)
    expect(maintenance.destroy).toHaveBeenCalledTimes(1)
    expect(() => sqsModule.getSQSMaintenanceClient()).toThrow('shut down')
  })

  it('builds the maintenance client with its own 16-socket pool', async () => {
    sqsModule.setSQSClient(undefined)
    const maintenance = sqsModule.getSQSMaintenanceClient()
    const main = sqsModule.getSQSClient()
    const agentOf = async client => (await client.config.requestHandler.configProvider).httpsAgent

    expect(maintenance).not.toBe(main)
    expect((await agentOf(maintenance)).maxSockets).toBe(16)
    expect((await agentOf(maintenance)).keepAlive).toBe(true)
    expect((await agentOf(main)).maxSockets).toBe(50)
    await sqsModule.shutdownSQSClient()
  })
})
