/* eslint-env jest */

import { jest } from '@jest/globals'

/** @returns {{promise: Promise<void>, resolve: () => void}} */
function gate () {
  let resolve
  const promise = new Promise(_resolve => { resolve = _resolve })
  return { promise, resolve }
}

/** @param {string} id @returns {object} */
function message (id) {
  return {
    MessageId: id,
    ReceiptHandle: `receipt-${id}`,
    Body: id,
    Attributes: { ApproximateReceiveCount: '1', MessageGroupId: 'same-group' }
  }
}

const QUEUE = 'custody-test_queue'
const QRL = 'https://sqs.us-east-1.amazonaws.com/123/custody-test_queue.fifo'

describe('qdone visibility-error custody', () => {
  let executor
  let sqs
  let extensionGate
  let extensionCode
  let killProcessGroup

  beforeEach(async () => {
    jest.resetModules()
    jest.useFakeTimers()
    jest.spyOn(console, 'error').mockImplementation(() => {})
    const { JobExecutor } = (await import('../src/scheduler/jobExecutor.js'))
    const { setSQSClient } = (await import('../src/sqs.js'))
    extensionGate = undefined
    extensionCode = 'RequestThrottled'
    sqs = {
      send: jest.fn(async command => {
        if (!command.input.Entries) return {}
        if (command.constructor.name === 'ChangeMessageVisibilityBatchCommand') {
          if (extensionGate) await extensionGate.promise
          return {
            Failed: command.input.Entries.map(({ Id }) => ({
              Id, Code: extensionCode, SenderFault: true
            }))
          }
        }
        return { Successful: command.input.Entries.map(({ Id }) => ({ Id })) }
      })
    }
    setSQSClient(sqs)
    killProcessGroup = jest.fn()
    executor = new JobExecutor({
      prefix: 'custody-test_',
      json: false,
      disableLog: true,
      killAfter: 3600,
      killProcessGroup,
      forceKillDelayMs: 1000
    })
  })

  afterEach(async () => {
    jest.clearAllTimers()
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  /** @param {string} id @param {boolean} fail @param {string} receiptHandle @returns {object} */
  function startJob (id = 'a', fail = false, receiptHandle = `receipt-${id}`) {
    const started = gate()
    const held = gate()
    const execution = executor.executeJobs([{ ...message(id), ReceiptHandle: receiptHandle }], async (_queue, _payload, attributes) => {
      attributes.registerPid(4321, { processGroup: true })
      started.resolve()
      await held.promise
      if (fail) throw new Error('Synthetic callback failure')
    }, QUEUE, QRL)
    return { started: started.promise, finish: held.resolve, execution }
  }

  /** @returns {Promise<void>} */
  async function failExtension () {
    executor.jobs.forEach(job => { job.extendAtSecond = 0 })
    await executor.maintainVisibility()
  }

  /** @returns {string[]} */
  function deletedIds () {
    return sqs.send.mock.calls.filter(([command]) => command.constructor.name === 'DeleteMessageBatchCommand')
      .flatMap(([command]) => command.input.Entries.map(({ Id }) => Id))
  }

  it('keeps a running child available to forced drain after an entry failure', async () => {
    const job = startJob()
    await job.started
    await failExtension()

    expect(executor.shutdownStatus()).toMatchObject({ activeJobs: 1, runningJobs: 1, trackedJobs: 1 })
    expect(executor.jobsByMessageId.a.status).toBe('running')
    expect(executor.jobsByQueue.get(QUEUE).size).toBe(1)
    const force = executor.forceTerminateActiveJobs()
    await jest.advanceTimersByTimeAsync(1000)
    await expect(force).resolves.toMatchObject({ attemptedJobs: 1, failedSignals: [] })
    expect(killProcessGroup.mock.calls).toEqual([[4321, 'SIGTERM'], [4321, 'SIGKILL']])
    job.finish()
    await job.execution
    await executor.shutdown()
    expect(deletedIds()).toEqual([])
  })

  it('still enforces kill-after after a visibility entry fails', async () => {
    executor.opt.killAfter = 1
    const job = startJob()
    await job.started
    await failExtension()
    await jest.advanceTimersByTimeAsync(6000)
    expect(killProcessGroup.mock.calls).toEqual([[4321, 'SIGTERM'], [4321, 'SIGKILL']])
    job.finish()
    await job.execution
    await executor.shutdown()
    expect(deletedIds()).toEqual([])
  })

  it('deletes a successful callback after a visibility error without abandoning its receipt', async () => {
    const job = startJob()
    await job.started
    await failExtension()
    job.finish()
    await job.execution
    await executor.shutdown()
    expect(deletedIds()).toEqual(['a'])
    expect(executor.shutdownStatus()).toMatchObject({ activeJobs: 0, trackedJobs: 0 })
  })

  it('does not acknowledge a failed callback after a visibility error', async () => {
    const job = startJob('a', true)
    await job.started
    await failExtension()
    job.finish()
    await job.execution
    await executor.shutdown()
    expect(deletedIds()).toEqual([])
    expect(executor.stats.jobsFailed).toBe(1)
  })

  it('retries a transient visibility failure before the original receipt expires', async () => {
    const job = startJob()
    await job.started
    await jest.advanceTimersByTimeAsync(60000)
    expect(sqs.send).toHaveBeenCalledTimes(1)
    sqs.send.mockImplementation(async command => ({
      Successful: command.input.Entries.map(({ Id }) => ({ Id }))
    }))
    await jest.advanceTimersByTimeAsync(9999)
    expect(sqs.send).toHaveBeenCalledTimes(1)
    await jest.advanceTimersByTimeAsync(1)
    expect(sqs.send).toHaveBeenCalledTimes(2)
    expect(executor.shutdownStatus()).toMatchObject({ activeJobs: 1, trackedJobs: 1 })
    job.finish()
    await job.execution
    await executor.shutdown()
    expect(deletedIds()).toEqual(['a'])
  })

  it('finishes local custody without counting an invalid receipt as acknowledged', async () => {
    sqs.send.mockImplementation(async command => ({
      Failed: command.input.Entries.map(({ Id }) => ({
        Id, Code: 'ReceiptHandleIsInvalid', SenderFault: true
      }))
    }))
    const job = startJob()
    await job.started
    await failExtension()
    expect(executor.shutdownStatus()).toMatchObject({ activeJobs: 1, trackedJobs: 1 })
    job.finish()
    await job.execution
    await executor.shutdown()
    expect(deletedIds()).toEqual(['a'])
    expect(executor.stats.jobsDeleted).toBe(0)
    expect(executor.shutdownStatus()).toMatchObject({ activeJobs: 0, trackedJobs: 0 })
  })

  it('bounds throttled retries during rapid FIFO completions without inflating the lease', async () => {
    const job = startJob()
    await job.started
    await failExtension()
    for (let index = 0; index < 20; index++) {
      await executor.executeJobs([message(`quick-${index}`)], async () => {}, QUEUE, QRL)
      await jest.advanceTimersByTimeAsync(1)
    }
    const extensionCalls = () => sqs.send.mock.calls
      .filter(([command]) => command.constructor.name === 'ChangeMessageVisibilityBatchCommand')
    expect(extensionCalls()).toHaveLength(1)
    expect(executor.jobsByMessageId.a.visibilityTimeout).toBe(120)
    await jest.advanceTimersByTimeAsync(10000)
    expect(extensionCalls()).toHaveLength(2)
    expect(extensionCalls().map(([command]) => command.input.Entries[0].VisibilityTimeout)).toEqual([240, 240])
    job.finish()
    await job.execution
    await executor.shutdown()
  })

  it('does not replace a newer lease update when an older extension fails', async () => {
    const job = startJob()
    await job.started
    extensionGate = gate()
    const maintenance = failExtension()
    await Promise.resolve()
    await executor.setJobVisibilityTimeout(executor.jobsByMessageId.a, 60)
    const newer = {
      visibilityTimeout: executor.jobsByMessageId.a.visibilityTimeout,
      extendAtSecond: executor.jobsByMessageId.a.extendAtSecond
    }
    extensionGate.resolve()
    await maintenance
    expect(executor.jobsByMessageId.a).toMatchObject(newer)
    job.finish()
    await job.execution
    await executor.shutdown()
  })

  it('skips a queued extension superseded by inline registration while another queue awaits SQS', async () => {
    const first = startJob()
    await first.started
    executor.opt.killAfter = 30
    const initialClamp = gate()
    const clampSent = gate()
    const earlierBatch = gate()
    const batchSent = gate()
    const inlineReady = gate()
    const inlineFinish = gate()
    const inlineQrl = `${QRL}-inline`
    sqs.send.mockImplementation(async command => {
      if (command.constructor.name === 'ChangeMessageVisibilityCommand' &&
        command.input.QueueUrl === inlineQrl && command.input.VisibilityTimeout === 30) {
        clampSent.resolve()
        await initialClamp.promise
      }
      if (command.constructor.name === 'ChangeMessageVisibilityBatchCommand' && command.input.QueueUrl === QRL) {
        batchSent.resolve()
        await earlierBatch.promise
      }
      return { Successful: command.input.Entries?.map(({ Id }) => ({ Id })) }
    })
    const inlineExecution = executor.executeJobs([message('inline')], async (_queue, _payload, attributes) => {
      await attributes.registerInlineExecution()
      inlineReady.resolve()
      await inlineFinish.promise
    }, `${QUEUE}-inline`, inlineQrl)
    await clampSent.promise
    const maintenance = failExtension()
    await batchSent.promise
    initialClamp.resolve()
    await inlineReady.promise
    earlierBatch.resolve()
    await maintenance
    const inlineLease = executor.jobsByMessageId.inline.visibilityTimeout
    const extensionInputs = sqs.send.mock.calls
      .filter(([command]) => command.constructor.name === 'ChangeMessageVisibilityBatchCommand')
      .map(([command]) => command.input)
    first.finish()
    inlineFinish.resolve()
    await Promise.all([first.execution, inlineExecution])
    await executor.shutdown()

    expect(inlineLease).toBe(120)
    expect(extensionInputs.some(input => input.QueueUrl === inlineQrl)).toBe(false)
    expect(extensionInputs.every(input => input.Entries.length > 0)).toBe(true)
    expect(deletedIds().sort()).toEqual(['a', 'inline'])
  })

  it.each(['ReceiptHandleIsInvalid', 'MessageNotInflight'])('stops retrying %s while preserving forced-drain custody', async code => {
    extensionCode = code
    const job = startJob()
    await job.started
    await failExtension()
    await jest.advanceTimersByTimeAsync(180000)
    expect(sqs.send).toHaveBeenCalledTimes(1)
    expect(executor.shutdownStatus()).toMatchObject({ activeJobs: 1, trackedJobs: 1 })
    const force = executor.forceTerminateActiveJobs()
    await jest.advanceTimersByTimeAsync(1000)
    await expect(force).resolves.toMatchObject({ attemptedJobs: 1, failedSignals: [] })
    job.finish()
    await job.execution
    await executor.shutdown()
    expect(deletedIds()).toEqual([])
  })

  it('does not apply a late permanent error to a new delivery of the same message', async () => {
    extensionCode = 'ReceiptHandleIsInvalid'
    const old = startJob()
    await old.started
    extensionGate = gate()
    const maintenance = failExtension()
    await Promise.resolve()
    old.finish()
    await old.execution
    await jest.advanceTimersByTimeAsync(1)
    expect(executor.jobsByMessageId.a).toBeUndefined()

    const fresh = startJob('a', false, 'receipt-a-new')
    await fresh.started
    const nextExtension = executor.jobsByMessageId.a.extendAtSecond
    extensionGate.resolve()
    await maintenance
    expect(executor.jobsByMessageId.a.extendAtSecond).toBe(nextExtension)
    expect(executor.jobsByMessageId.a.status).toBe('running')
    fresh.finish()
    await fresh.execution
    await executor.shutdown()
    expect(deletedIds()).toEqual(['a', 'a'])
  })

  it('does not resume invalid-receipt extensions when a waiting FIFO callback starts', async () => {
    extensionCode = 'ReceiptHandleIsInvalid'
    const firstStarted = gate()
    const releaseFirst = gate()
    const execution = executor.executeJobs([message('first'), message('second')], async (_queue, payload) => {
      if (payload === 'first') {
        firstStarted.resolve()
        await releaseFirst.promise
      }
    }, QUEUE, QRL)
    await firstStarted.promise
    await failExtension()
    executor.jobsByMessageId.second.visibilityTimeout = 7200
    releaseFirst.resolve()
    await execution
    await executor.shutdown()
    expect(sqs.send.mock.calls.some(([command]) => command.constructor.name === 'ChangeMessageVisibilityCommand'))
      .toBe(false)
    expect(deletedIds().sort()).toEqual(['first', 'second'])
  })

  it('does not overwrite completion when a failed extension returns late', async () => {
    const job = startJob()
    await job.started
    extensionGate = gate()
    const maintenance = failExtension()
    await Promise.resolve()
    job.finish()
    await job.execution
    extensionGate.resolve()
    await maintenance
    expect(executor.jobsByMessageId.a.status).toBe('complete')
    await executor.shutdown()
    expect(deletedIds()).toEqual(['a'])
  })

  it('retains waiting FIFO jobs through visibility failures until their callbacks run', async () => {
    const firstStarted = gate()
    const releaseFirst = gate()
    const payloads = []
    const execution = executor.executeJobs([message('first'), message('second')], async (_queue, payload) => {
      payloads.push(payload)
      if (payload === 'first') {
        firstStarted.resolve()
        await releaseFirst.promise
      }
    }, QUEUE, QRL)
    await firstStarted.promise
    await failExtension()
    expect(executor.shutdownStatus()).toMatchObject({ activeJobs: 2, runningJobs: 1, waitingJobs: 1, trackedJobs: 2 })
    const shutdown = executor.shutdown()
    releaseFirst.resolve()
    await Promise.all([execution, shutdown])
    expect(payloads).toEqual(['first', 'second'])
    expect(deletedIds().sort()).toEqual(['first', 'second'])
  })

  it('keeps duplicate-message admission guarded while the first callback is active', async () => {
    const job = startJob()
    await job.started
    await failExtension()
    await expect(executor.executeJobs([message('a')], async () => {}, QUEUE, QRL))
      .rejects.toThrow('Saw job a twice')
    job.finish()
    await job.execution
    await executor.shutdown()
    expect(deletedIds()).toEqual(['a'])
  })
})
