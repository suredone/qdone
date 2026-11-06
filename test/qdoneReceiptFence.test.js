/* eslint-env jest */
import { jest } from '@jest/globals'
import { spawn } from 'node:child_process'
import { once } from 'node:events'

/** @returns {{promise: Promise<void>, resolve: () => void}} */
function gate () {
  let resolve
  const promise = new Promise(_resolve => { resolve = _resolve })
  return { promise, resolve }
}

/** @param {string} id @param {string} group @param {string} receipt @returns {object} */
function message (id, group = 'serial', receipt = `receipt-${id}`) {
  return {
    MessageId: id,
    ReceiptHandle: receipt,
    Body: id,
    MessageAttributes: { QdoneDeduplicationId: { StringValue: `dedup-${id}` } },
    Attributes: { MessageGroupId: group, ApproximateReceiveCount: '1' }
  }
}

const QUEUE = 'fence_queue'
const QRL = 'https://sqs.us-east-1.amazonaws.com/123/fence_queue.fifo'
const terminalCodes = ['ReceiptHandleIsInvalid', 'MessageNotInflight']

describe('positively lost receipts before callback entry', () => {
  let JobExecutor
  let executor
  let sqs
  let redis
  let held
  let executions
  let shutdownCache

  beforeEach(async () => {
    jest.resetModules()
    jest.useFakeTimers()
    jest.spyOn(console, 'error').mockImplementation(() => {})
    ;({ JobExecutor } = await import('../src/scheduler/jobExecutor.js'))
    const { setSQSClient } = await import('../src/sqs.js')
    ;({ shutdownCache } = await import('../src/cache.js'))
    sqs = { send: jest.fn(async command => ({ Successful: command.input.Entries?.map(({ Id }) => ({ Id })) })) }
    setSQSClient(sqs)
    redis = { eval: jest.fn(async () => 1), quit: jest.fn(async () => 'OK') }
    executor = new JobExecutor({
      prefix: 'fence_',
      disableLog: true,
      killAfter: 3600,
      cacheUri: 'redis://synthetic.invalid',
      cachePrefix: 'test:',
      Redis: function FakeRedis () { return redis },
      killProcessGroup: jest.fn(),
      forceKillDelayMs: 1
    })
    held = []
    executions = []
  })

  afterEach(async () => {
    held.forEach(item => item.resolve())
    await Promise.allSettled(executions)
    await executor.shutdown()
    await shutdownCache()
    jest.clearAllTimers()
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  /** @returns {{promise: Promise<void>, resolve: () => void}} */
  function hold () {
    const item = gate()
    held.push(item)
    return item
  }

  /** @param {object[]} messages @param {function} callback @returns {Promise<void>} */
  function execute (messages, callback) {
    const execution = executor.executeJobs(messages, callback, QUEUE, QRL)
    executions.push(execution)
    return execution
  }

  /** @returns {object[]} */
  function deletes () {
    return sqs.send.mock.calls.filter(([command]) => command.constructor.name === 'DeleteMessageBatchCommand')
      .flatMap(([command]) => command.input.Entries)
  }

  /** @returns {Promise<void>} */
  async function extend () {
    executor.jobs.forEach(job => { job.extendAtSecond = 0 })
    await executor.maintainVisibility()
  }

  it.each(terminalCodes)('skips only lost B on %s while A/C and independent D progress and soft-drain', async code => {
    const entered = []
    const first = hold()
    const started = gate()
    sqs.send.mockImplementation(async command => {
      if (command.constructor.name === 'ChangeMessageVisibilityBatchCommand') {
        return {
          Successful: command.input.Entries.filter(entry => entry.Id !== 'B').map(({ Id }) => ({ Id })),
          Failed: [{ Id: 'B', Code: code, SenderFault: true }]
        }
      }
      return { Successful: command.input.Entries?.map(({ Id }) => ({ Id })) }
    })
    const execution = execute([message('A'), message('B'), message('C'), message('D', 'independent')], async (_queue, id) => {
      entered.push(id)
      if (id === 'A') { started.resolve(); await first.promise }
    })
    await started.promise
    const oldB = executor.jobsByMessageId.B
    await extend()
    expect(entered).toEqual(['A', 'D'])
    const stateAfterLoss = executor.shutdownStatus()
    const indexedAfterLoss = executor.jobsByMessageId.B
    const groupedAfterLoss = executor.jobsByQueue.get(QUEUE).has(oldB)
    const shutdown = executor.shutdown()
    first.resolve()
    await Promise.all([execution, shutdown])
    expect(entered).toEqual(['A', 'D', 'C'])
    expect(stateAfterLoss).toMatchObject({ activeJobs: 2, waitingJobs: 1, runningJobs: 1 })
    expect(indexedAfterLoss).toBeUndefined()
    expect(oldB.status).toBe('abandoned')
    expect(groupedAfterLoss).toBe(false)
    expect(deletes().map(entry => entry.ReceiptHandle).sort()).toEqual(['receipt-A', 'receipt-C', 'receipt-D'])
    expect(redis.eval.mock.calls.map(([, , key]) => key).sort()).toEqual(['test:dedup:dedup-A', 'test:dedup:dedup-C', 'test:dedup:dedup-D'])
    expect(sqs.send.mock.calls.every(([command]) => command.constructor.name !== 'SendMessageCommand')).toBe(true)
    expect(sqs.send.mock.calls.every(([command]) => command.input.VisibilityTimeout !== 0 &&
      !command.input.Entries?.some(entry => entry.VisibilityTimeout === 0))).toBe(true)
    expect(executor.stats).toMatchObject({ activeJobs: 0, waitingJobs: 0, runningJobs: 0, jobsSkipped: 1, jobsFailed: 0, jobsSucceeded: 3, jobsDeleted: 3 })
    expect(executor.shutdownStatus()).toMatchObject({ trackedJobs: 0, trackedBatches: 0 })
  })

  it.each(terminalCodes)('skips callback on single-clamp %s after internal running begins', async code => {
    executor.opt.killAfter = 30
    const clamp = hold()
    const sent = gate()
    sqs.send.mockImplementation(async command => {
      if (command.constructor.name === 'ChangeMessageVisibilityCommand') {
        sent.resolve()
        await clamp.promise
        throw Object.assign(new Error('synthetic typed service failure'), { name: code })
      }
      return {}
    })
    const callback = jest.fn()
    const execution = execute([message('B')], callback)
    await sent.promise
    expect(executor.shutdownStatus()).toMatchObject({ activeJobs: 1, runningJobs: 1, waitingJobs: 0 })
    expect(callback).not.toHaveBeenCalled()
    clamp.resolve()
    await execution
    await executor.shutdown()
    expect(callback).not.toHaveBeenCalled()
    expect(deletes()).toEqual([])
    expect(redis.eval).not.toHaveBeenCalled()
    expect(executor.stats).toMatchObject({ jobsSkipped: 1, jobsSucceeded: 0, jobsFailed: 0, activeJobs: 0, runningJobs: 0, waitingJobs: 0 })
  })

  it.each(terminalCodes)('waits for a held batch %s before deciding a callback clamp', async code => {
    const first = hold()
    const started = gate()
    const callback = jest.fn(async (_queue, id) => {
      if (id === 'A') { started.resolve(); await first.promise }
    })
    const execution = execute([message('A'), message('B'), message('C')], callback)
    await started.promise
    const response = hold()
    const sent = gate()
    sqs.send.mockImplementation(async command => {
      if (command.constructor.name === 'ChangeMessageVisibilityBatchCommand') {
        sent.resolve()
        await response.promise
        return { Failed: [{ Id: 'B', Code: code }] }
      }
      return { Successful: command.input.Entries?.map(({ Id }) => ({ Id })) }
    })
    executor.jobsByMessageId.B.extendAtSecond = 0
    const maintenance = executor.maintainVisibility()
    await sent.promise
    executor.opt.killAfter = 30
    first.resolve()
    await jest.advanceTimersByTimeAsync(0)
    expect(callback.mock.calls.map(([, id]) => id)).toEqual(['A'])
    expect(sqs.send.mock.calls.some(([command]) => command.input.ReceiptHandle === 'receipt-B')).toBe(false)
    response.resolve()
    await Promise.all([maintenance, execution])
    await executor.shutdown()
    expect(callback.mock.calls.map(([, id]) => id)).toEqual(['A', 'C'])
    expect(deletes().map(entry => entry.ReceiptHandle).sort()).toEqual(['receipt-A', 'receipt-C'])
    expect(redis.eval.mock.calls.map(([, , key]) => key).sort()).toEqual(['test:dedup:dedup-A', 'test:dedup:dedup-C'])
    expect(executor.stats).toMatchObject({ jobsSkipped: 1, activeJobs: 0, runningJobs: 0, waitingJobs: 0 })
  })

  it.each(['RequestThrottled', 'Error'])('preserves pre-clamp dispatch on %s even with terminal words in message', async name => {
    executor.opt.killAfter = 30
    sqs.send.mockImplementation(async command => {
      if (command.constructor.name === 'ChangeMessageVisibilityCommand') {
        throw Object.assign(new Error('ReceiptHandleIsInvalid MessageNotInflight'), { name })
      }
      return { Successful: command.input.Entries?.map(({ Id }) => ({ Id })) }
    })
    const callback = jest.fn()
    await execute([message('B')], callback)
    await executor.shutdown()
    expect(callback).toHaveBeenCalledTimes(1)
    expect(deletes().map(entry => entry.ReceiptHandle)).toEqual(['receipt-B'])
    expect(executor.stats.jobsSkipped).toBe(0)
  })

  it('admits fresh B while old B is tombstoned and does not erase its active duplicate guard', async () => {
    const first = hold()
    const started = gate()
    const entered = []
    const oldExecution = execute([message('A'), message('B'), message('C')], async (_queue, id) => {
      entered.push(id)
      if (id === 'A') { started.resolve(); await first.promise }
    })
    await started.promise
    const oldB = executor.jobsByMessageId.B
    const late = hold()
    const sent = gate()
    sqs.send.mockImplementation(async command => {
      if (command.constructor.name === 'ChangeMessageVisibilityBatchCommand') {
        sent.resolve()
        await late.promise
        return { Failed: [{ Id: 'B', Code: 'ReceiptHandleIsInvalid' }] }
      }
      return { Successful: command.input.Entries?.map(({ Id }) => ({ Id })) }
    })
    oldB.extendAtSecond = 0
    const latePass = executor.maintainVisibility()
    await sent.promise
    late.resolve()
    await latePass
    expect(oldB.status).toBe('abandoned')
    const freshHeld = hold()
    const freshStarted = gate()
    const freshExecution = execute([message('B', 'serial', 'receipt-B-fresh')], async () => {
      entered.push('B-fresh'); freshStarted.resolve(); await freshHeld.promise
    })
    await freshStarted.promise
    const freshB = executor.jobsByMessageId.B
    first.resolve()
    await oldExecution
    await executor.maintainVisibility()
    expect(entered).toEqual(['A', 'B-fresh', 'C'])
    expect(executor.jobsByMessageId.B).toBe(freshB)
    await expect(execute([message('B', 'serial', 'receipt-B-duplicate')], async () => {})).rejects.toThrow('Saw job B twice')
    freshHeld.resolve()
    await freshExecution
    await executor.shutdown()
    expect(deletes().filter(entry => entry.Id === 'B').map(entry => entry.ReceiptHandle)).toEqual(['receipt-B-fresh'])
    expect(executor.stats).toMatchObject({ jobsSkipped: 1, jobsSucceeded: 3, activeJobs: 0, waitingJobs: 0, runningJobs: 0 })
  })

  it('does not submit an explicit change after a pending batch confirms receipt loss', async () => {
    const first = hold()
    const started = gate()
    const callback = jest.fn(async (_queue, id) => {
      if (id === 'A') { started.resolve(); await first.promise }
    })
    const execution = execute([message('A'), message('B')], callback)
    await started.promise
    const response = hold()
    const sent = gate()
    sqs.send.mockImplementation(async command => {
      if (command.constructor.name === 'ChangeMessageVisibilityBatchCommand') {
        sent.resolve(); await response.promise
        return { Failed: [{ Id: 'B', Code: 'MessageNotInflight' }] }
      }
      return { Successful: command.input.Entries?.map(({ Id }) => ({ Id })) }
    })
    executor.jobsByMessageId.B.extendAtSecond = 0
    const maintenance = executor.maintainVisibility()
    await sent.promise
    const update = executor.setJobVisibilityTimeout(executor.jobsByMessageId.B, 360)
    expect(sqs.send.mock.calls.some(([command]) => command.input.ReceiptHandle === 'receipt-B')).toBe(false)
    response.resolve()
    await Promise.all([maintenance, update])
    expect(sqs.send.mock.calls.some(([command]) => command.input.ReceiptHandle === 'receipt-B')).toBe(false)
    first.resolve()
    await execution
    await executor.shutdown()
    expect(callback.mock.calls.map(([, id]) => id)).toEqual(['A'])
    expect(executor.stats.jobsSkipped).toBe(1)
  })

  it.each(['missing', 'unknown-entry', 'sender-fault', 'whole-batch'])('does not infer receipt loss from %s', async mode => {
    const first = hold()
    const started = gate()
    const entered = []
    const execution = execute([message('A'), message('B')], async (_queue, id) => {
      entered.push(id)
      if (id === 'A') { started.resolve(); await first.promise }
    })
    await started.promise
    sqs.send.mockImplementation(async command => {
      if (command.constructor.name === 'ChangeMessageVisibilityBatchCommand') {
        if (mode === 'whole-batch') throw Object.assign(new Error('terminal phrase'), { name: 'ReceiptHandleIsInvalid' })
        if (mode === 'unknown-entry') return { Failed: [{ Id: 'unknown', Code: 'MessageNotInflight' }] }
        if (mode === 'sender-fault') return { Failed: [{ Id: 'B', Code: 'RequestThrottled', SenderFault: true }] }
        return {}
      }
      return { Successful: command.input.Entries?.map(({ Id }) => ({ Id })) }
    })
    await extend()
    first.resolve()
    await execution
    await executor.shutdown()
    expect(entered).toEqual(['A', 'B'])
    expect(executor.stats.jobsSkipped).toBe(0)
    expect(deletes().map(entry => entry.Id).sort()).toEqual(['A', 'B'])
  })

  it.each(['inline', 'before-pid', 'child'])('retains entered %s custody and duplicate guard on permanent loss', async mode => {
    const started = gate()
    const finish = hold()
    const execution = execute([message('A')], async (_queue, _id, attributes) => {
      if (mode === 'inline') await attributes.registerInlineExecution()
      if (mode === 'child') attributes.registerPid(4321, { processGroup: true })
      started.resolve()
      await finish.promise
    })
    await started.promise
    sqs.send.mockImplementation(async command => command.constructor.name === 'ChangeMessageVisibilityBatchCommand'
      ? { Failed: [{ Id: 'A', Code: 'ReceiptHandleIsInvalid' }] }
      : { Successful: command.input.Entries?.map(({ Id }) => ({ Id })) })
    await extend()
    expect(executor.jobsByMessageId.A.callbackEntered).toBe(true)
    expect(executor.shutdownStatus()).toMatchObject({ activeJobs: 1, runningJobs: 1 })
    await expect(execute([message('A', 'serial', 'fresh-A')], async () => {})).rejects.toThrow('Saw job A twice')
    if (mode === 'child') {
      const force = executor.forceTerminateActiveJobs()
      await jest.advanceTimersByTimeAsync(1)
      await expect(force).resolves.toMatchObject({ attemptedJobs: 1 })
      expect(executor.opt.killProcessGroup.mock.calls).toEqual([[4321, 'SIGTERM'], [4321, 'SIGKILL']])
    }
    finish.resolve()
    await execution
    await executor.shutdown()
    expect(executor.stats).toMatchObject({ jobsSkipped: 0, activeJobs: 0, runningJobs: 0 })
    expect(deletes().map(entry => entry.Id)).toEqual(mode === 'child' ? [] : ['A'])
  })

  it('retains and drains an owned inert child after callback entry and permanent receipt loss', async () => {
    jest.useRealTimers()
    const started = gate()
    let child
    let childExit
    executor.opt.killProcessGroup = (pid, signal) => process.kill(-pid, signal)
    executor.opt.forceKillDelayMs = 10
    const execution = execute([message('child')], async (_queue, _id, attributes) => {
      child = spawn(process.execPath, ['-e', "process.stdout.write('ready'); setInterval(() => {}, 1000)"], {
        detached: true, stdio: ['ignore', 'pipe', 'ignore'], env: {}
      })
      childExit = once(child, 'exit')
      attributes.registerPid(child.pid, { processGroup: true })
      await once(child.stdout, 'data')
      started.resolve()
      await childExit
      throw new Error('Owned fixture child terminated')
    })
    try {
      await started.promise
      sqs.send.mockImplementation(async command => ({ Failed: command.input.Entries?.map(({ Id }) => ({ Id, Code: 'MessageNotInflight' })) }))
      await extend()
      expect(executor.jobsByMessageId.child.pid).toBe(child.pid)
      expect(executor.stats.activeJobs).toBe(1)
      await expect(executor.forceTerminateActiveJobs()).resolves.toMatchObject({ attemptedJobs: 1, failedSignals: [] })
      await execution
      await executor.shutdown()
      expect(deletes()).toEqual([])
      expect(executor.stats).toMatchObject({ activeJobs: 0, runningJobs: 0, jobsKilled: 1, jobsFailed: 1, jobsSkipped: 0 })
    } finally {
      if (child && child.exitCode === null && child.signalCode === null) {
        try { process.kill(-child.pid, 'SIGKILL') } catch (error) { expect(error.code).toBe('ESRCH') }
      }
      if (childExit) await childExit
    }
  })

  it('settles a controlled two-executor handoff only on the new broker receipt', async () => {
    const first = hold()
    const started = gate()
    const effects = []
    const oldExecution = execute([message('A'), message('B')], async (_queue, id) => {
      effects.push(`old:${id}`)
      if (id === 'A') { started.resolve(); await first.promise }
    })
    await started.promise
    sqs.send.mockImplementation(async command => command.constructor.name === 'ChangeMessageVisibilityBatchCommand'
      ? { Failed: [{ Id: 'B', Code: 'ReceiptHandleIsInvalid' }] }
      : { Successful: command.input.Entries?.map(({ Id }) => ({ Id })) })
    await extend()
    const nextExecutor = new JobExecutor(executor.opt)
    try {
      await nextExecutor.executeJobs([message('B', 'serial', 'receipt-B-next-owner')], async () => { effects.push('new:B') }, QUEUE, QRL)
      await nextExecutor.shutdown()
      first.resolve()
      await oldExecution
      await executor.shutdown()
      expect(effects).toEqual(['old:A', 'new:B'])
      expect(deletes().filter(entry => entry.Id === 'B').map(entry => entry.ReceiptHandle)).toEqual(['receipt-B-next-owner'])
      expect(nextExecutor.stats).toMatchObject({ jobsSucceeded: 1, jobsDeleted: 1, activeJobs: 0 })
      expect(executor.stats).toMatchObject({ jobsSucceeded: 1, jobsSkipped: 1, activeJobs: 0 })
    } finally {
      first.resolve()
      await nextExecutor.shutdown()
    }
  })
})
