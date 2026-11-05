/* eslint-env jest */
import { jest } from '@jest/globals'

const FIFO = 'https://sqs.us-east-1.amazonaws.com/123/lease_fifo.fifo'
const PEER = 'https://sqs.us-east-1.amazonaws.com/123/lease_peer'

/** @returns {{promise: Promise<void>, resolve: () => void}} */
function gate () {
  let resolve
  const promise = new Promise(_resolve => { resolve = _resolve })
  return { promise, resolve }
}

/** @param {string} id @param {string} group @param {string} receipt @returns {object} */
function message (id, group = id, receipt = 'receipt-' + id) {
  return {
    MessageId: id,
    ReceiptHandle: receipt,
    Body: id,
    Attributes: { MessageGroupId: group },
    MessageAttributes: { QdoneDeduplicationId: { StringValue: 'dedup-' + id } }
  }
}

/** @param {{Entries: {Id: string}[]}} input @returns {{Successful: {Id: string}[]}} */
function success (input) {
  return { Successful: input.Entries.map(({ Id }) => ({ Id })) }
}

describe('confirmed visibility renewal', () => {
  let executor
  let sqs
  let redis
  let closeCache
  let renew
  let single
  let ack
  let gates
  let executions
  let effects

  beforeEach(async () => {
    jest.resetModules()
    jest.useFakeTimers({ now: 0 })
    jest.spyOn(console, 'error').mockImplementation(() => {})
    const { JobExecutor } = await import('../src/scheduler/jobExecutor.js')
    const { setSQSClient } = await import('../src/sqs.js')
    const { shutdownCache } = await import('../src/cache.js')
    closeCache = shutdownCache
    renew = success
    single = () => ({})
    ack = success
    sqs = {
      send: jest.fn(async command => {
        const input = command.input
        switch (command.constructor.name) {
          case 'ChangeMessageVisibilityBatchCommand': return renew(input)
          case 'ChangeMessageVisibilityCommand': return single(input)
          case 'DeleteMessageBatchCommand': return ack(input)
          default: throw new Error('Unexpected SQS command')
        }
      })
    }
    setSQSClient(sqs)
    redis = { del: jest.fn(async () => 1), quit: jest.fn(async () => 'OK') }
    executor = new JobExecutor({
      prefix: 'lease_',
      killAfter: 3600,
      disableLog: true,
      cacheUri: 'redis://synthetic.invalid',
      cachePrefix: 'test:',
      Redis: function FakeRedis () { return redis }
    })
    gates = []
    executions = []
    effects = []
  })

  afterEach(async () => {
    gates.forEach(item => item.resolve())
    await Promise.allSettled(executions)
    await executor.shutdown()
    await closeCache()
    jest.clearAllTimers()
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  /** @returns {{promise: Promise<void>, resolve: () => void}} */
  function hold () {
    const item = gate()
    gates.push(item)
    return item
  }

  /** @param {object[]} messages @param {function} callback @param {string} qrl @returns {Promise<void>} */
  function execute (messages, callback, qrl = FIFO) {
    expect(messages.length).toBeLessThanOrEqual(10)
    const promise = executor.executeJobs(messages, callback, qrl.slice(qrl.lastIndexOf('/') + 1), qrl)
    executions.push(promise)
    return promise
  }

  /** @param {object[]} messages @param {{promise: Promise<void>}} finish @param {string} qrl @returns {Promise<void>} */
  function start (messages, finish, qrl = FIFO) {
    return execute(messages, async (_queue, id) => {
      effects.push(id)
      await finish.promise
    }, qrl)
  }

  /** @param {string} name @returns {object[]} */
  function calls (name) {
    return sqs.send.mock.calls.filter(([command]) => command.constructor.name === name)
      .map(([command]) => command.input)
  }

  /** @returns {object[]} */
  function renewals () {
    return calls('ChangeMessageVisibilityBatchCommand')
  }

  /** @returns {object[]} */
  function deletes () {
    return calls('DeleteMessageBatchCommand').flatMap(input => input.Entries)
  }

  it('retries a whole rejected renewal from the last confirmed duration', async () => {
    const finish = hold()
    start([message('A')], finish)
    renew = () => { throw new Error('synthetic transport failure') }
    await jest.advanceTimersByTimeAsync(60000)
    const job = executor.jobsByMessageId.A
    expect(renewals()).toHaveLength(1)
    expect(job.visibilityTimeout).toBe(120)
    expect(job).toMatchObject({ confirmedVisibilitySentAt: null, extendAtSecond: 60, visibilityRetryAt: 70000 })
    expect(executor.stats.timeoutsExtended).toBe(0)
    expect(job.visibilityRequest).toBeUndefined()

    renew = success
    await jest.advanceTimersByTimeAsync(9999)
    expect(renewals()).toHaveLength(1)
    await jest.advanceTimersByTimeAsync(1)
    expect(renewals().map(input => input.Entries[0].VisibilityTimeout)).toEqual([240, 240])
    expect(job).toMatchObject({ visibilityTimeout: 240, confirmedVisibilitySentAt: 70000, extendAtSecond: 190 })
    expect(job.visibilityRetryAt).toBeUndefined()
    expect(executor.stats.timeoutsExtended).toBe(1)
  })

  it('submits receipt eleven before the first ten renewals settle', async () => {
    const finish = hold()
    const first = hold()
    const messages = Array.from({ length: 11 }, (_, index) => message(String(index)))
    const submitted = []
    renew = async input => {
      submitted.push({ time: Date.now(), ids: input.Entries.map(entry => entry.Id) })
      if (input.Entries[0].Id === '0') await first.promise
      return success(input)
    }
    const batches = [start(messages.slice(0, 10), finish), start(messages.slice(10), finish)]
    await jest.advanceTimersByTimeAsync(60000)
    expect(submitted).toEqual([
      { time: 60000, ids: messages.slice(0, 10).map(item => item.MessageId) },
      { time: 60000, ids: ['10'] }
    ])
    expect(executor.jobsByMessageId['0'].visibilityTimeout).toBe(120)
    expect(executor.jobsByMessageId['10']).toMatchObject({ visibilityTimeout: 240, confirmedVisibilitySentAt: 60000 })
    await jest.advanceTimersByTimeAsync(61000)
    first.resolve()
    await jest.advanceTimersByTimeAsync(0)
    expect(executor.jobsByMessageId['0']).toMatchObject({
      visibilityTimeout: 240, confirmedVisibilitySentAt: 60000, extendAtSecond: 180
    })
    finish.resolve()
    await Promise.all(batches)
    await executor.shutdown()
    expect(effects.sort()).toEqual(messages.map(item => item.MessageId).sort())
    expect(deletes().map(entry => entry.Id).sort()).toEqual(effects)
    expect(redis.del).toHaveBeenCalledTimes(11)
  })

  it('correlates partial replies and refuses unknown, duplicate and conflicting IDs', async () => {
    const finish = hold()
    const ids = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    const execution = start(ids.map(id => message(id, ['A', 'C', 'D', 'F'].includes(id) ? 'fifo' : id)), finish)
    renew = () => ({
      Successful: ['A', 'E', 'E', 'F', 'unknown'].map(Id => ({ Id })),
      Failed: [
        { Id: 'B', Code: 'RequestThrottled', SenderFault: true },
        { Id: 'C', Code: 'ReceiptHandleIsInvalid' },
        { Id: 'D', Code: 'MessageNotInflight' },
        { Id: 'F', Code: 'MessageNotInflight' }
      ]
    })
    await jest.advanceTimersByTimeAsync(60000)
    expect(executor.jobsByMessageId.A.visibilityTimeout).toBe(240)
    for (const id of ['B', 'E', 'F', 'G']) {
      expect(executor.jobsByMessageId[id]).toMatchObject({
        visibilityTimeout: 120, confirmedVisibilitySentAt: null, extendAtSecond: 60, visibilityRetryAt: 70000
      })
    }
    expect(executor.jobsByMessageId.C).toBeUndefined()
    expect(executor.jobsByMessageId.D).toBeUndefined()
    expect(executor.stats).toMatchObject({ jobsSkipped: 2, timeoutsExtended: 1 })
    finish.resolve()
    await execution
    await executor.shutdown()
    expect(effects.sort()).toEqual(['A', 'B', 'E', 'F', 'G'])
    expect(deletes().map(entry => entry.Id).sort()).toEqual(effects)
  })

  it.each([null, { Successful: [{ Id: 'A' }], Failed: 'malformed' }])('leaves an ambiguous batch reply unconfirmed: %j', async reply => {
    const finish = hold()
    start([message('A')], finish)
    renew = () => reply
    await jest.advanceTimersByTimeAsync(60000)
    expect(executor.jobsByMessageId.A).toMatchObject({
      visibilityTimeout: 120, confirmedVisibilitySentAt: null, extendAtSecond: 60, visibilityRetryAt: 70000
    })
    expect(executor.stats).toMatchObject({ jobsSkipped: 0, timeoutsExtended: 0 })
  })

  it('recomputes a FIFO clamp after its own held renewal confirms', async () => {
    const first = hold()
    const pending = hold()
    const execution = execute([message('A', 'serial'), message('B', 'serial')], async (_queue, id) => {
      effects.push(id)
      if (id === 'A') await first.promise
    })
    renew = async input => { await pending.promise; return success(input) }
    await jest.advanceTimersByTimeAsync(60000)
    expect(renewals()[0].Entries.find(entry => entry.Id === 'B').VisibilityTimeout).toBe(240)
    executor.opt.killAfter = 200
    first.resolve()
    await jest.advanceTimersByTimeAsync(20000)
    expect(effects).toEqual(['A'])
    expect(calls('ChangeMessageVisibilityCommand')).toEqual([])
    pending.resolve()
    await execution
    expect(calls('ChangeMessageVisibilityCommand')).toEqual([
      { QueueUrl: FIFO, ReceiptHandle: 'receipt-B', VisibilityTimeout: 180 }
    ])
    expect(effects).toEqual(['A', 'B'])
  })

  it('keeps a disabled clamp from delaying FIFO entry behind its own renewal', async () => {
    executor.opt.killAfter = 0
    const first = hold()
    const pending = hold()
    const execution = execute([message('A', 'serial'), message('B', 'serial')], async (_queue, id) => {
      effects.push(id)
      if (id === 'A') await first.promise
    })
    renew = async input => { await pending.promise; return success(input) }
    await jest.advanceTimersByTimeAsync(60000)
    first.resolve()
    await execution
    expect(effects).toEqual(['A', 'B'])
    expect(calls('ChangeMessageVisibilityCommand')).toEqual([])
    pending.resolve()
  })

  it('waits for a held short renewal before restoring inline visibility', async () => {
    executor.opt.killAfter = 30
    const beginInline = hold()
    const response = hold()
    const execution = execute([message('A')], async (_queue, _id, attributes) => {
      await beginInline.promise
      await attributes.registerInlineExecution()
    })
    renew = async input => { await response.promise; return success(input) }
    await jest.advanceTimersByTimeAsync(20000)
    expect(renewals()[0].Entries[0].VisibilityTimeout).toBe(10)
    beginInline.resolve()
    await jest.advanceTimersByTimeAsync(0)
    expect(calls('ChangeMessageVisibilityCommand').map(input => input.VisibilityTimeout)).toEqual([30])
    response.resolve()
    await execution
    expect(calls('ChangeMessageVisibilityCommand').map(input => input.VisibilityTimeout)).toEqual([30, 120])
  })

  it.each([[30, false], [1, false], [1, true]])('restores a short %is clamp (confirmed=%s) after an ambiguous inline reply', async (killAfter, confirmed) => {
    executor.opt.killAfter = killAfter
    const ready = gate()
    const finish = hold()
    single = input => {
      if (confirmed && input.VisibilityTimeout === killAfter) return {}
      throw new Error('synthetic ambiguous reply')
    }
    execute([message('A')], async (_queue, _id, attributes) => {
      await attributes.registerInlineExecution()
      ready.resolve()
      await finish.promise
    })
    await ready.promise
    const job = executor.jobsByMessageId.A
    expect(calls('ChangeMessageVisibilityCommand').map(input => input.VisibilityTimeout)).toEqual([killAfter, 120])
    expect(job.visibilityTimeout).toBe(confirmed ? killAfter : 120)
    expect(job.confirmedVisibilitySentAt).toBe(confirmed ? 0 : null)
    if (!confirmed) expect(job.possiblyShortVisibility).toBe(true)
    expect(job.visibilityRequest).toBeUndefined()
    await jest.advanceTimersByTimeAsync(9999)
    expect(renewals()).toEqual([])
    await jest.advanceTimersByTimeAsync(1)
    expect(renewals()[0].Entries[0].VisibilityTimeout).toBe(120)
    expect(job).toMatchObject({ visibilityTimeout: 120, confirmedVisibilitySentAt: 10000, status: 'running' })
    expect(job.possiblyShortVisibility).toBeUndefined()
  })

  it('interleaves a due peer and caps overlapping background renewals at four', async () => {
    const finish = hold()
    const pending = Array.from({ length: 4 }, () => hold())
    let active = 0
    let peak = 0
    let count = 0
    renew = async input => {
      const index = count++
      peak = Math.max(peak, ++active)
      if (pending[index]) await pending[index].promise
      active--
      return success(input)
    }
    for (let i = 0; i < 5; i++) {
      start(Array.from({ length: 10 }, (_, j) => message('large-' + (i * 10 + j))), finish)
    }
    start([message('peer')], finish, PEER)
    await jest.advanceTimersByTimeAsync(60000)
    expect(renewals().map(input => input.QueueUrl)).toEqual([FIFO, PEER, FIFO, FIFO])
    expect(executor.jobsByMessageId['large-49']).toMatchObject({
      visibilityTimeout: 120, confirmedVisibilitySentAt: null, extendAtSecond: 60
    })
    expect(executor.jobsByMessageId['large-49'].visibilityRequest).toBeUndefined()
    await executor.maintainVisibility()
    expect(renewals()).toHaveLength(4)
    pending.slice(1).forEach(item => item.resolve())
    await jest.advanceTimersByTimeAsync(0)
    const ids = renewals().flatMap(input => input.Entries.map(entry => entry.Id))
    expect(ids).toHaveLength(51)
    expect(new Set(ids).size).toBe(51)
    expect(renewals().every(input => input.Entries.length <= 10)).toBe(true)
    expect(active).toBe(1)
    expect(peak).toBe(4)
    pending[0].resolve()
  })

  it('confirms an ACK in the same pass while another receipt renewal is held', async () => {
    const finish = hold()
    const response = hold()
    start([message('A')], finish)
    await execute([message('done')], async () => {})
    executor.jobsByMessageId.A.extendAtSecond = 0
    renew = async input => { await response.promise; return success(input) }
    const maintenance = executor.maintainVisibility()
    await jest.advanceTimersByTimeAsync(0)
    expect(renewals()).toHaveLength(1)
    expect(deletes().map(entry => entry.Id)).toEqual(['done'])
    expect(redis.del).toHaveBeenCalledWith('test:dedup:dedup-done')
    let settled = false
    const shutdown = executor.shutdown().then(() => { settled = true })
    finish.resolve()
    await jest.advanceTimersByTimeAsync(0)
    expect(settled).toBe(false)
    response.resolve()
    await Promise.all([maintenance, shutdown])
    expect(deletes().map(entry => entry.Id)).toEqual(['done', 'A'])
  })

  it('retains the prior pending ACK cleanup boundary and captured receipt', async () => {
    const oldAck = hold()
    const pending = hold()
    const finish = hold()
    ack = async input => {
      if (input.Entries.some(entry => entry.ReceiptHandle === 'receipt-A')) await oldAck.promise
      return success(input)
    }
    await execute([message('A')], async () => {})
    const oldPass = executor.maintainVisibility()
    start([message('long')], finish)
    executor.jobsByMessageId.long.extendAtSecond = 0
    renew = async input => { await pending.promise; return success(input) }
    const laterPass = executor.maintainVisibility()
    await jest.advanceTimersByTimeAsync(0)
    await expect(execute([message('A', 'A', 'fresh-A')], async () => {})).rejects.toThrow('Saw job A twice')
    expect(redis.del).not.toHaveBeenCalled()
    pending.resolve()
    await laterPass
    expect(executor.jobsByMessageId.A).toBeUndefined()
    start([message('A', 'A', 'fresh-A')], finish)
    const fresh = executor.jobsByMessageId.A
    oldAck.resolve()
    await oldPass
    expect(executor.jobsByMessageId.A).toBe(fresh)
    expect(redis.del.mock.calls.map(([key]) => key)).toEqual(['test:dedup:dedup-A'])
    finish.resolve()
    await executor.shutdown()
    expect(deletes().map(entry => entry.ReceiptHandle).sort()).toEqual(['fresh-A', 'receipt-A', 'receipt-long'])
  })

  it('conserves 1000 mixed FIFO and standard callbacks, renewals and ACKs', async () => {
    const finish = hold()
    const batches = []
    for (let batch = 0; batch < 100; batch++) {
      const messages = Array.from({ length: 10 }, (_, index) => {
        const id = 'mixed-' + (batch * 10 + index)
        return message(id, index < 6 ? 'serial-' + batch : id)
      })
      batches.push(start(messages, finish, batch % 2 ? FIFO : PEER))
    }
    await jest.advanceTimersByTimeAsync(60000)
    expect(renewals()).toHaveLength(100)
    expect(executor.shutdownStatus()).toMatchObject({ activeJobs: 1000, waitingJobs: 250, runningJobs: 750 })
    expect(executor.stats.timeoutsExtended).toBe(1000)
    finish.resolve()
    await Promise.all(batches)
    await executor.shutdown()
    expect(effects).toHaveLength(1000)
    expect(new Set(effects).size).toBe(1000)
    expect(deletes().map(entry => entry.Id).sort()).toEqual(effects.sort())
    expect(redis.del).toHaveBeenCalledTimes(1000)
    expect(executor.stats).toMatchObject({ activeJobs: 0, waitingJobs: 0, runningJobs: 0, jobsSucceeded: 1000, jobsDeleted: 1000 })
  })
})
