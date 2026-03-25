import {
  ChangeMessageVisibilityCommand,
  ChangeMessageVisibilityBatchCommand,
  DeleteMessageBatchCommand
} from '@aws-sdk/client-sqs'
import { mockClient } from 'aws-sdk-client-mock'
import 'aws-sdk-client-mock-jest'
import { jest } from '@jest/globals'

import { JobExecutor } from '../src/scheduler/jobExecutor.js'
import { getSQSClient, setSQSClient } from '../src/sqs.js'
import { getOptionsWithDefaults } from '../src/defaults.js'
import { shutdownCache } from '../src/cache.js'

getSQSClient()
const client = getSQSClient()

const cacheOptions = {
  cacheUri: 'redis://localhost',
  Redis: (await import('ioredis-mock')).default
}
const executors = []

function makeMessage (id = 'test-msg-1') {
  return {
    MessageId: id,
    Body: '/usr/bin/php /var/sdapp/suredone/jobs/test.php',
    ReceiptHandle: 'receipt-' + id,
    Attributes: {
      ApproximateReceiveCount: '1',
      SentTimestamp: String(Date.now()),
      ApproximateFirstReceiveTimestamp: String(Date.now())
    }
  }
}

function makeExecutor (overrides = {}) {
  const killTree = overrides.killTree || jest.fn((_pid, _signal, callback) => callback?.())
  const opt = getOptionsWithDefaults({
    ...cacheOptions,
    killAfter: 60,
    prefix: 'sdqd_',
    disableLog: true,
    ...overrides
  })
  opt.killTree = killTree
  // Create executor but immediately clear the maintenance timer
  // so it doesn't run during tests
  const executor = new JobExecutor(opt)
  clearTimeout(executor.maintainVisibilityTimeout)
  executors.push(executor)
  return executor
}

function cleanupExecutors () {
  for (const executor of executors.splice(0)) {
    clearTimeout(executor.maintainVisibilityTimeout)
    for (const job of executor.jobs) {
      clearTimeout(job.killTimer)
      clearTimeout(job.killSignalTimer)
    }
  }
}

describe('JobExecutor kill-after', () => {
  let sqsMock

  beforeEach(() => {
    shutdownCache()
    sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock.on(ChangeMessageVisibilityCommand).resolves({})
    sqsMock.on(ChangeMessageVisibilityBatchCommand).resolves({ Successful: [], Failed: [] })
    sqsMock.on(DeleteMessageBatchCommand).resolves({ Successful: [], Failed: [] })
  })

  afterEach(() => {
    cleanupExecutors()
    jest.useRealTimers()
    sqsMock.restore()
  })

  afterAll(shutdownCache)

  test('job exceeding killAfter with registered PID is killed', async () => {
    const killTree = jest.fn((_pid, _signal, callback) => callback?.())
    const executor = makeExecutor({ killAfter: 10, killTree })
    const msg = makeMessage('kill-test')
    const callback = async () => {}

    const job = executor.addJob(msg, callback, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')
    // Simulate runJob having started
    job.status = 'running'
    job.executionStart = new Date(Date.now() - 15000) // 15s ago, exceeds killAfter=10
    job.pid = 12345

    await executor.maintainVisibility()

    expect(job.killed).toBe(true)
    expect(executor.stats.jobsKilled).toBe(1)
    expect(killTree).toHaveBeenCalledWith(12345, 'SIGTERM', expect.any(Function))
  })

  test('jobsKilled stat is correctly incremented', async () => {
    const killTree = jest.fn((_pid, _signal, callback) => callback?.())
    const executor = makeExecutor({ killAfter: 5, killTree })

    // Add two jobs that both exceed killAfter
    for (const id of ['kill-1', 'kill-2']) {
      const job = executor.addJob(makeMessage(id), async () => {}, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')
      job.status = 'running'
      job.executionStart = new Date(Date.now() - 10000)
      job.pid = 99990 + parseInt(id.split('-')[1])
    }

    await executor.maintainVisibility()

    expect(executor.stats.jobsKilled).toBe(2)
    expect(killTree).toHaveBeenCalledTimes(2)
  })

  test('visibility timeout is capped by killAfter', async () => {
    const executor = makeExecutor({ killAfter: 120 })
    const msg = makeMessage('vis-cap-test')
    const job = executor.addJob(msg, async () => {}, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')

    // Simulate running for 100s with killAfter=120 — 20s remaining
    job.status = 'running'
    job.executionStart = new Date(Date.now() - 100000)
    job.extendAtSecond = 0 // Force extension to fire this cycle

    await executor.maintainVisibility()

    // Visibility should be capped: min(doubled=240, secondsUntilMax=huge, secondsUntilKill=~20)
    expect(job.visibilityTimeout).toBeLessThanOrEqual(25) // ~20s with some timing tolerance
    expect(job.visibilityTimeout).toBeGreaterThan(0)
  })

  test('runJob shrinks the initial visibility timeout to killAfter', async () => {
    const executor = makeExecutor({ killAfter: 30 })
    const msg = makeMessage('initial-visibility')
    const job = executor.addJob(msg, async () => {}, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')

    await executor.runJob(job)

    expect(job.visibilityTimeout).toBe(30)
    expect(job.extendAtSecond).toBe(15)
    expect(sqsMock).toHaveReceivedCommandTimes(ChangeMessageVisibilityCommand, 1)
    expect(sqsMock.commandCalls(ChangeMessageVisibilityCommand)[0].args[0].input).toEqual({
      QueueUrl: 'https://sqs/sdqd_testqueue',
      ReceiptHandle: msg.ReceiptHandle,
      VisibilityTimeout: 30
    })
  })

  test('job without registered PID is not killed but visibility is still capped', async () => {
    const executor = makeExecutor({ killAfter: 60 })
    const msg = makeMessage('no-pid-test')
    const job = executor.addJob(msg, async () => {}, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')

    // Simulate running for 90s past killAfter — no PID registered
    job.status = 'running'
    job.executionStart = new Date(Date.now() - 90000)
    job.extendAtSecond = 0

    await executor.maintainVisibility()

    // Should NOT be killed (no PID)
    expect(job.killed).toBeUndefined()
    expect(executor.stats.jobsKilled).toBe(0)

    // But visibility should be capped to 1 (since we're past killAfter)
    expect(job.visibilityTimeout).toBe(1)
  })

  test('registerInlineExecution restores default visibility for inline jobs', async () => {
    const executor = makeExecutor({ killAfter: 30 })
    const msg = makeMessage('inline-restore')
    const job = executor.addJob(msg, async (_queue, _payload, attributes) => {
      await attributes.registerInlineExecution()
    }, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')

    await executor.runJob(job)

    expect(job.executionMode).toBe('inline')
    expect(job.visibilityTimeout).toBe(120)
    expect(sqsMock).toHaveReceivedCommandTimes(ChangeMessageVisibilityCommand, 2)
    expect(sqsMock.commandCalls(ChangeMessageVisibilityCommand).map(call => call.args[0].input.VisibilityTimeout)).toEqual([30, 120])
  })

  test('registerInlineExecution is ignored after registerPid', async () => {
    const executor = makeExecutor({ killAfter: 30 })
    const msg = makeMessage('pid-first')
    const job = executor.addJob(msg, async (_queue, _payload, attributes) => {
      attributes.registerPid(12345)
      await attributes.registerInlineExecution()
    }, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')

    await executor.runJob(job)

    expect(job.executionMode).toBe('child_process')
    expect(job.pid).toBe(12345)
    expect(job.visibilityTimeout).toBe(30)
    expect(sqsMock).toHaveReceivedCommandTimes(ChangeMessageVisibilityCommand, 1)
  })

  test('registerPid is ignored after registerInlineExecution', async () => {
    const executor = makeExecutor({ killAfter: 30 })
    const msg = makeMessage('inline-first')
    const job = executor.addJob(msg, async (_queue, _payload, attributes) => {
      await attributes.registerInlineExecution()
      attributes.registerPid(12345)
    }, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')

    await executor.runJob(job)

    expect(job.executionMode).toBe('inline')
    expect(job.pid).toBeUndefined()
    expect(job.visibilityTimeout).toBe(120)
    expect(sqsMock).toHaveReceivedCommandTimes(ChangeMessageVisibilityCommand, 2)
    expect(sqsMock.commandCalls(ChangeMessageVisibilityCommand).map(call => call.args[0].input.VisibilityTimeout)).toEqual([30, 120])
  })

  test('registerInlineExecution is idempotent', async () => {
    const executor = makeExecutor({ killAfter: 30 })
    const msg = makeMessage('inline-idempotent')
    const job = executor.addJob(msg, async (_queue, _payload, attributes) => {
      await attributes.registerInlineExecution()
      await attributes.registerInlineExecution()
    }, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')

    await executor.runJob(job)

    expect(job.executionMode).toBe('inline')
    expect(job.visibilityTimeout).toBe(120)
    expect(sqsMock).toHaveReceivedCommandTimes(ChangeMessageVisibilityCommand, 2)
    expect(sqsMock.commandCalls(ChangeMessageVisibilityCommand).map(call => call.args[0].input.VisibilityTimeout)).toEqual([30, 120])
  })

  test('inline jobs exceeding killAfter are not visibility capped or killed', async () => {
    const executor = makeExecutor({ killAfter: 60 })
    const msg = makeMessage('inline-overrun')
    const job = executor.addJob(msg, async () => {}, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')

    job.status = 'running'
    job.executionStart = new Date(Date.now() - 90000)
    job.executionMode = 'inline'
    job.extendAtSecond = 0

    await executor.maintainVisibility()

    expect(job.visibilityTimeout).toBe(240)
    expect(job.killed).toBeUndefined()
    expect(executor.stats.jobsKilled).toBe(0)
    expect(job.inlineKillAfterLogged).toBe(true)
  })

  test('job completing within killAfter is not affected', async () => {
    const executor = makeExecutor({ killAfter: 3600 })
    const msg = makeMessage('ok-test')
    const job = executor.addJob(msg, async () => {}, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')

    // Simulate running for 10s with killAfter=3600
    job.status = 'running'
    job.executionStart = new Date(Date.now() - 10000)
    job.pid = 12345
    job.extendAtSecond = 0

    await executor.maintainVisibility()

    expect(job.killed).toBeUndefined()
    expect(executor.stats.jobsKilled).toBe(0)
    // Visibility should be doubled, not capped
    expect(job.visibilityTimeout).toBe(240) // 120 * 2
  })

  test('registerPid validates input', async () => {
    const executor = makeExecutor()
    const msg = makeMessage('validate-pid')
    const job = executor.addJob(msg, async (_queue, _payload, attributes) => {
      attributes.registerPid(0)
      expect(job.pid).toBeUndefined()
      attributes.registerPid(1)
      expect(job.pid).toBeUndefined()
      attributes.registerPid(-5)
      expect(job.pid).toBeUndefined()
      attributes.registerPid('123')
      expect(job.pid).toBeUndefined()
      attributes.registerPid(process.pid)
      expect(job.pid).toBeUndefined()

      attributes.registerPid(12345)
      expect(job.pid).toBe(12345)
    }, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')

    await executor.runJob(job)
  })

  test('killed flag prevents double-kill', async () => {
    const killTree = jest.fn((_pid, _signal, callback) => callback?.())
    const executor = makeExecutor({ killAfter: 5, killTree })
    const msg = makeMessage('double-kill')
    const job = executor.addJob(msg, async () => {}, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')

    job.status = 'running'
    job.executionStart = new Date(Date.now() - 10000)
    job.pid = 12345

    // First maintenance cycle — should kill
    await executor.maintainVisibility()
    expect(job.killed).toBe(true)
    expect(executor.stats.jobsKilled).toBe(1)
    expect(killTree).toHaveBeenCalledTimes(1)

    // Second maintenance cycle — should NOT kill again
    await executor.maintainVisibility()
    expect(executor.stats.jobsKilled).toBe(1) // still 1
    expect(killTree).toHaveBeenCalledTimes(1)
  })

  test('maintainVisibility waits for the exact killAfter deadline', async () => {
    jest.useFakeTimers()
    jest.setSystemTime(new Date('2026-03-24T00:00:10.000Z'))
    const killTree = jest.fn((_pid, _signal, callback) => callback?.())
    const executor = makeExecutor({ killAfter: 10, killTree })
    const msg = makeMessage('exact-maintenance-deadline')
    const job = executor.addJob(msg, async () => {}, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')

    job.status = 'running'
    job.executionStart = new Date(Date.now() - 9500)
    job.pid = 12345

    await executor.maintainVisibility()
    expect(job.killed).toBeUndefined()
    expect(executor.stats.jobsKilled).toBe(0)
    expect(killTree).not.toHaveBeenCalled()

    jest.setSystemTime(new Date(Date.now() + 500))

    await executor.maintainVisibility()
    expect(job.killed).toBe(true)
    expect(executor.stats.jobsKilled).toBe(1)
    expect(killTree).toHaveBeenCalledWith(12345, 'SIGTERM', expect.any(Function))
  })

  test('killAfter uses the exact deadline instead of waiting for maintenance', async () => {
    const killTree = jest.fn((pid, signal, callback) => callback?.())
    jest.useFakeTimers()
    const executor = makeExecutor({ killAfter: 1, killTree })

    const promise = executor.executeJobs(
      [makeMessage('exact-deadline')],
      async (_queue, _payload, attributes) => {
        attributes.registerPid(12345)
        await new Promise(resolve => setTimeout(resolve, 2000))
      },
      'sdqd_testqueue',
      'https://sqs/sdqd_testqueue'
    )

    await jest.advanceTimersByTimeAsync(999)
    expect(killTree).not.toHaveBeenCalled()

    await jest.advanceTimersByTimeAsync(1)
    expect(killTree).toHaveBeenCalledWith(12345, 'SIGTERM', expect.any(Function))

    await jest.advanceTimersByTimeAsync(1000)
    await promise
    expect(executor.stats.jobsKilled).toBe(1)
  })

  test('waiting FIFO job does not get visibility capped', async () => {
    const executor = makeExecutor({ killAfter: 60 })
    const msg = makeMessage('fifo-wait')
    const job = executor.addJob(msg, async () => {}, 'sdqd_testqueue.fifo', 'https://sqs/sdqd_testqueue.fifo')

    // Job is still waiting (no executionStart), received 90s ago
    job.status = 'waiting'
    job.start = new Date(Date.now() - 90000)
    job.extendAtSecond = 0

    await executor.maintainVisibility()

    // Visibility should NOT be capped by killAfter since executionStart is not set
    // It should double normally: 120 * 2 = 240
    expect(job.visibilityTimeout).toBe(240)
    expect(job.killed).toBeUndefined()
  })

  test('shutdown logs and exits immediately when idle', async () => {
    const errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {})
    const executor = makeExecutor({ verbose: true })
    executor.maintainPromise = Promise.resolve()

    await executor.shutdown()

    expect(executor.shutdownRequested).toBe(true)
    expect(errorSpy.mock.calls.flat().join(' ')).toContain('Shutting down jobExecutor')
    expect(errorSpy.mock.calls.flat().join(' ')).toContain('All workers done, finishing shutdown of jobExecutor')

    errorSpy.mockRestore()
  })

  test('job counters report active and running jobs per queue', () => {
    const executor = makeExecutor()
    const runningJob = executor.addJob(makeMessage('running-job'), async () => {}, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')
    const waitingJob = executor.addJob(makeMessage('waiting-job'), async () => {}, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')
    const otherQueueJob = executor.addJob(makeMessage('other-queue-job'), async () => {}, 'sdqd_otherqueue', 'https://sqs/sdqd_otherqueue')

    runningJob.status = 'running'
    waitingJob.status = 'waiting'
    otherQueueJob.status = 'running'
    executor.stats.runningJobs = 2

    expect(executor.activeJobCount()).toBe(3)
    expect(executor.runningJobCount()).toBe(2)
    expect(executor.runningJobCountForQueue('sdqd_testqueue')).toBe(1)
    expect(executor.runningJobCountForQueue('sdqd_otherqueue')).toBe(1)
    expect(executor.runningJobCountForQueue('sdqd_missing')).toBe(0)
  })

  test('killJob logs kill-after events and escalates to SIGKILL when pid stays alive', async () => {
    jest.useFakeTimers()
    const killTree = jest.fn((_pid, _signal, callback) => callback?.())
    const logSpy = jest.spyOn(console, 'log').mockImplementation(() => {})
    const processKillSpy = jest.spyOn(process, 'kill').mockImplementation(() => true)
    const executor = makeExecutor({ killAfter: 5, killTree, disableLog: false })
    const job = executor.addJob(makeMessage('kill-escalation'), async () => {}, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')

    job.status = 'running'
    job.executionStart = new Date(Date.now() - 6000)
    job.pid = 12345

    executor.killJob(job, new Date())

    expect(logSpy).toHaveBeenCalledWith(expect.stringContaining('"event":"JOB_KILL_AFTER"'))
    expect(killTree).toHaveBeenCalledWith(12345, 'SIGTERM', expect.any(Function))

    await jest.advanceTimersByTimeAsync(5000)

    expect(processKillSpy).toHaveBeenCalledWith(12345, 0)
    expect(killTree).toHaveBeenCalledWith(12345, 'SIGKILL', expect.any(Function))

    processKillSpy.mockRestore()
    logSpy.mockRestore()
  })

  test('setRunningVisibilityTimeout logs failures in verbose mode', async () => {
    const errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {})
    sqsMock.on(ChangeMessageVisibilityCommand).rejects(new Error('boom'))
    const executor = makeExecutor({ killAfter: 30, verbose: true })
    const job = executor.addJob(makeMessage('visibility-failure'), async () => {}, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')

    job.visibilityTimeout = 120

    await executor.setRunningVisibilityTimeout(job)

    expect(errorSpy).toHaveBeenCalledWith(expect.stringContaining('FAILED_TO_SET_VISIBILITY_TIMEOUT'), expect.any(Object))

    errorSpy.mockRestore()
  })

  test('maintainVisibility logs stats and handles extension batch success and failure', async () => {
    const errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {})
    const executor = makeExecutor({ verbose: true })
    const failedJob = executor.addJob(makeMessage('extend-failed'), async () => {}, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')
    const successfulJob = executor.addJob(makeMessage('extend-successful'), async () => {}, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')
    const staleFailedJob = executor.addJob(makeMessage('stale-failed'), async () => {}, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')

    failedJob.status = 'running'
    failedJob.executionStart = new Date()
    failedJob.extendAtSecond = 0
    successfulJob.status = 'running'
    successfulJob.executionStart = new Date()
    successfulJob.extendAtSecond = 0
    staleFailedJob.status = 'failed'
    executor.stats.runningJobs = 2
    executor.stats.waitingJobs = 0

    sqsMock.on(ChangeMessageVisibilityBatchCommand).resolves({
      Failed: [{ Id: failedJob.message.MessageId }],
      Successful: [{ Id: successfulJob.message.MessageId }]
    })

    await executor.maintainVisibility()

    expect(executor.stats.timeoutsExtended).toBe(1)
    expect(executor.jobsByMessageId[failedJob.message.MessageId]).toBeUndefined()
    expect(executor.jobsByMessageId[successfulJob.message.MessageId]).toBeDefined()
    expect(executor.jobsByMessageId[staleFailedJob.message.MessageId]).toBeUndefined()
    expect(errorSpy.mock.calls.flat().join(' ')).toContain('FAILED_TO_EXTEND_JOB')
    expect(errorSpy.mock.calls.flat().join(' ')).toContain('Extended')

    errorSpy.mockRestore()
  })

  test('maintainVisibility deletes completed jobs and logs batch results', async () => {
    const errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {})
    const logSpy = jest.spyOn(console, 'log').mockImplementation(() => {})
    const executor = makeExecutor({ disableLog: false })
    const deleteSuccessJob = executor.addJob(makeMessage('delete-success'), async () => {}, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')
    const deleteFailedJob = executor.addJob(makeMessage('delete-failed'), async () => {}, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')

    deleteSuccessJob.status = 'complete'
    deleteFailedJob.status = 'complete'

    sqsMock.on(DeleteMessageBatchCommand).resolves({
      Failed: [{ Id: deleteFailedJob.message.MessageId }],
      Successful: [{ Id: deleteSuccessJob.message.MessageId }]
    })

    await executor.maintainVisibility()

    expect(executor.stats.jobsDeleted).toBe(1)
    expect(executor.jobsByMessageId[deleteSuccessJob.message.MessageId]).toBeUndefined()
    expect(executor.jobsByMessageId[deleteFailedJob.message.MessageId]).toBeUndefined()
    expect(logSpy).toHaveBeenCalledWith(expect.stringContaining('"event":"DELETE_MESSAGES"'))
    expect(errorSpy.mock.calls.flat().join(' ')).toContain('FAILED_TO_DELETE_JOB')

    logSpy.mockRestore()
    errorSpy.mockRestore()
  })

  test('addJob logs message receipt and rejects duplicate message ids', () => {
    const logSpy = jest.spyOn(console, 'log').mockImplementation(() => {})
    const executor = makeExecutor({ disableLog: false })
    const msg = makeMessage('duplicate-message')
    const originalJob = executor.addJob(msg, async () => {}, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')

    expect(logSpy).toHaveBeenCalledWith(expect.stringContaining('"event":"MESSAGE_RECEIVED"'))

    let duplicateError
    try {
      executor.addJob(msg, async () => {}, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')
    } catch (err) {
      duplicateError = err
    }

    expect(duplicateError.message).toContain('Saw job duplicate-message twice')
    expect(duplicateError.job).toBe(originalJob)

    logSpy.mockRestore()
  })

  test('runJob logs success and failure events when logging is enabled', async () => {
    const logSpy = jest.spyOn(console, 'log').mockImplementation(() => {})
    const executor = makeExecutor({ disableLog: false })
    const successJob = executor.addJob(makeMessage('run-success'), async () => 'ok', 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')
    const failedJob = executor.addJob(makeMessage('run-failure'), async () => {
      throw new Error('boom')
    }, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')

    await executor.runJob(successJob)
    await executor.runJob(failedJob)

    const logs = logSpy.mock.calls.map(call => call[0])
    expect(logs.some(line => line.includes('"event":"MESSAGE_PROCESSING_START"'))).toBe(true)
    expect(logs.some(line => line.includes('"event":"MESSAGE_PROCESSING_COMPLETE"'))).toBe(true)
    expect(logs.some(line => line.includes('"event":"MESSAGE_PROCESSING_FAILED"'))).toBe(true)

    logSpy.mockRestore()
  })

  test('runJob logs verbose success output', async () => {
    const errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {})
    const executor = makeExecutor({ verbose: true })
    const job = executor.addJob(makeMessage('verbose-success'), async () => 'ok', 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')

    await executor.runJob(job)

    const output = errorSpy.mock.calls.flat().join(' ')
    expect(output).toContain('Got message:')
    expect(output).toContain('Running:')
    expect(output).toContain('SUCCESS')
    expect(output).toContain('done')

    errorSpy.mockRestore()
  })

  test('executeJobs refuses new work while shutting down', async () => {
    const executor = makeExecutor()
    executor.shutdownRequested = true

    await expect(
      executor.executeJobs([makeMessage('shutdown-block')], async () => {}, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')
    ).rejects.toThrow('jobExecutor is shutting down so cannot execute new jobs')
  })
})
