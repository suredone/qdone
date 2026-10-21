/* eslint-env jest */

import { jest } from '@jest/globals'

import { JobExecutor } from '../src/scheduler/jobExecutor.js'
import { setSQSClient } from '../src/sqs.js'

describe('qdone forced child termination', () => {
  it('still escalates a timed-out process group after its root callback exits', async () => {
    jest.useFakeTimers()
    const sqsClient = { send: jest.fn() }
    setSQSClient(sqsClient)

    let rejectJob
    const jobGate = new Promise((_resolve, reject) => { rejectJob = reject })
    const killProcessGroup = jest.fn((_pid, signal) => {
      if (signal === 'SIGTERM') rejectJob(new Error('root exited after SIGTERM'))
    })
    const executor = new JobExecutor({
      prefix: 'escalation-test_',
      json: false,
      verbose: false,
      disableLog: true,
      killAfter: 1,
      killProcessGroup
    })

    let markStarted
    const started = new Promise(resolve => { markStarted = resolve })
    const execution = executor.executeJobs([{
      MessageId: 'escalation-message',
      ReceiptHandle: 'escalation-receipt',
      Body: 'escalation-payload',
      Attributes: { ApproximateReceiveCount: '1' }
    }], async (_queue, _payload, attributes) => {
      attributes.registerPid(4321, { processGroup: true })
      markStarted()
      await jobGate
    }, 'escalation-test_queue', 'https://sqs.us-east-1.amazonaws.com/123/escalation-test_queue')

    await started
    await jest.advanceTimersByTimeAsync(1000)
    await execution
    expect(killProcessGroup.mock.calls.map(([_pid, signal]) => signal)).toEqual(['SIGTERM'])

    let shutdownResolved = false
    const shutdown = executor.shutdown().then(() => { shutdownResolved = true })
    await jest.advanceTimersByTimeAsync(4999)
    expect(shutdownResolved).toBe(false)
    await jest.advanceTimersByTimeAsync(1)
    await shutdown

    expect(killProcessGroup.mock.calls.map(([_pid, signal]) => signal)).toEqual(['SIGTERM', 'SIGKILL'])
    expect(sqsClient.send.mock.calls.some(([command]) => command.constructor.name === 'DeleteMessageBatchCommand'))
      .toBe(false)
    jest.useRealTimers()
  })

  it('awaits the grace period and kills the process group before leaving a resolved callback undeleted', async () => {
    jest.useFakeTimers()
    const sqsClient = { send: jest.fn() }
    setSQSClient(sqsClient)

    const killProcessGroup = jest.fn()
    const executor = new JobExecutor({
      prefix: 'force-test_',
      json: false,
      verbose: false,
      disableLog: true,
      killAfter: 3600,
      forceKillDelayMs: 1000,
      killProcessGroup
    })

    let markStarted
    let releaseJob
    const started = new Promise(resolve => { markStarted = resolve })
    const jobGate = new Promise(resolve => { releaseJob = resolve })
    const execution = executor.executeJobs([{
      MessageId: 'forced-message',
      ReceiptHandle: 'forced-receipt',
      Body: 'forced-payload',
      Attributes: { ApproximateReceiveCount: '1' }
    }], async (_queue, _payload, attributes) => {
      attributes.registerPid(4321, { processGroup: true })
      markStarted()
      await jobGate
    }, 'force-test_queue', 'https://sqs.us-east-1.amazonaws.com/123/force-test_queue')

    await started
    let forceResolved = false
    const forceTermination = executor.forceTerminateActiveJobs().then(result => {
      forceResolved = true
      return result
    })
    await Promise.resolve()
    expect(killProcessGroup).toHaveBeenCalledWith(4321, 'SIGTERM')
    expect(forceResolved).toBe(false)

    await jest.advanceTimersByTimeAsync(999)
    expect(forceResolved).toBe(false)
    await jest.advanceTimersByTimeAsync(1)
    await expect(forceTermination).resolves.toMatchObject({ attemptedJobs: 1, failedSignals: [] })
    expect(killProcessGroup.mock.calls.map(([_pid, signal]) => signal)).toEqual(['SIGTERM', 'SIGKILL'])

    releaseJob()
    await expect(execution).resolves.toBeUndefined()
    await expect(executor.shutdown()).resolves.toBeUndefined()
    expect(sqsClient.send).not.toHaveBeenCalled()
    jest.useRealTimers()
  })

  it('keeps concurrent shutdown pending until forced group termination finishes', async () => {
    jest.useFakeTimers()
    const sqsClient = { send: jest.fn() }
    setSQSClient(sqsClient)

    let rejectJob
    const jobGate = new Promise((_resolve, reject) => { rejectJob = reject })
    const killProcessGroup = jest.fn((_pid, signal) => {
      if (signal === 'SIGTERM') rejectJob(new Error('root exited during forced termination'))
    })
    const executor = new JobExecutor({
      prefix: 'force-shutdown-test_',
      json: false,
      verbose: false,
      disableLog: true,
      killAfter: 3600,
      forceKillDelayMs: 1000,
      killProcessGroup
    })

    let markStarted
    const started = new Promise(resolve => { markStarted = resolve })
    const execution = executor.executeJobs([{
      MessageId: 'force-shutdown-message',
      ReceiptHandle: 'force-shutdown-receipt',
      Body: 'force-shutdown-payload',
      Attributes: { ApproximateReceiveCount: '1' }
    }], async (_queue, _payload, attributes) => {
      attributes.registerPid(4321, { processGroup: true })
      markStarted()
      await jobGate
    }, 'force-shutdown-test_queue', 'https://sqs.us-east-1.amazonaws.com/123/force-shutdown-test_queue')

    await started
    let shutdownResolved = false
    const shutdown = executor.shutdown().then(() => { shutdownResolved = true })
    const forceTermination = executor.forceTerminateActiveJobs()
    await execution
    expect(killProcessGroup.mock.calls.map(([_pid, signal]) => signal)).toEqual(['SIGTERM'])

    await jest.advanceTimersByTimeAsync(999)
    expect(shutdownResolved).toBe(false)
    await jest.advanceTimersByTimeAsync(1)
    await expect(Promise.all([shutdown, forceTermination])).resolves.toEqual([
      undefined,
      expect.objectContaining({ attemptedJobs: 1, failedSignals: [] })
    ])
    expect(killProcessGroup.mock.calls.map(([_pid, signal]) => signal)).toEqual(['SIGTERM', 'SIGKILL'])
    expect(sqsClient.send).not.toHaveBeenCalled()
    jest.useRealTimers()
  })

  it('waits for tree-kill callbacks and reports a failed signal', async () => {
    const executor = new JobExecutor({
      prefix: 'tree-test_',
      json: false,
      verbose: false,
      disableLog: true,
      killAfter: 3600
    })
    let signalCallback
    const killTree = jest.fn((_pid, _signal, callback) => { signalCallback = callback })
    let signalResolved = false
    const signal = executor.signalJobTree(killTree, 4321, 'SIGTERM').then(result => {
      signalResolved = true
      return result
    })

    await Promise.resolve()
    expect(signalResolved).toBe(false)
    signalCallback(new Error('signal failed'))
    await expect(signal).resolves.toMatchObject({ signal: 'SIGTERM', succeeded: false, error: 'signal failed' })
    await executor.shutdown()
  })

  it('owns every accepted same-group FIFO job through shutdown and deletes them after completion', async () => {
    const sqsClient = {
      send: jest.fn(command => ({
        Successful: command.input.Entries.map(entry => ({ Id: entry.Id }))
      }))
    }
    setSQSClient(sqsClient)

    const executor = new JobExecutor({
      prefix: 'fifo-test_',
      json: false,
      verbose: false,
      disableLog: true,
      killAfter: 0
    })
    const startedPayloads = []
    let markFirstStarted
    let releaseFirst
    const firstStarted = new Promise(resolve => { markFirstStarted = resolve })
    const firstGate = new Promise(resolve => { releaseFirst = resolve })
    const messages = ['first', 'second'].map(payload => ({
      MessageId: `fifo-${payload}`,
      ReceiptHandle: `receipt-${payload}`,
      Body: payload,
      Attributes: { MessageGroupId: 'same-group' }
    }))

    const execution = executor.executeJobs(messages, async (_queue, payload) => {
      startedPayloads.push(payload)
      if (payload === 'first') {
        markFirstStarted()
        await firstGate
      }
    }, 'fifo-test_queue', 'https://sqs.us-east-1.amazonaws.com/123/fifo-test_queue.fifo')

    await firstStarted
    const shutdown = executor.shutdown()
    await expect(Promise.race([
      shutdown.then(() => 'resolved'),
      new Promise(resolve => setTimeout(() => resolve('draining'), 25))
    ])).resolves.toBe('draining')
    expect(startedPayloads).toEqual(['first'])

    releaseFirst()
    await expect(Promise.all([execution, shutdown])).resolves.toEqual([undefined, undefined])
    expect(startedPayloads).toEqual(['first', 'second'])
    expect(sqsClient.send).toHaveBeenCalledTimes(1)
    expect(sqsClient.send.mock.calls[0][0].constructor.name).toBe('DeleteMessageBatchCommand')
  })
})
