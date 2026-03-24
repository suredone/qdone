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
  const opt = getOptionsWithDefaults({
    ...cacheOptions,
    killAfter: 60,
    prefix: 'sdqd_',
    disableLog: true,
    ...overrides
  })
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
    const executor = makeExecutor({ killAfter: 10 })
    const msg = makeMessage('kill-test')
    const callback = async () => {}

    const job = executor.addJob(msg, callback, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')
    // Simulate runJob having started
    job.status = 'running'
    job.executionStart = new Date(Date.now() - 15000) // 15s ago, exceeds killAfter=10
    job.pid = 99999 // fake PID — treeKill will fail but that's fine

    await executor.maintainVisibility()

    expect(job.killed).toBe(true)
    expect(executor.stats.jobsKilled).toBe(1)
  })

  test('jobsKilled stat is correctly incremented', async () => {
    const executor = makeExecutor({ killAfter: 5 })

    // Add two jobs that both exceed killAfter
    for (const id of ['kill-1', 'kill-2']) {
      const job = executor.addJob(makeMessage(id), async () => {}, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')
      job.status = 'running'
      job.executionStart = new Date(Date.now() - 10000)
      job.pid = 99990 + parseInt(id.split('-')[1])
    }

    await executor.maintainVisibility()

    expect(executor.stats.jobsKilled).toBe(2)
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
    const executor = makeExecutor({ killAfter: 5 })
    const msg = makeMessage('double-kill')
    const job = executor.addJob(msg, async () => {}, 'sdqd_testqueue', 'https://sqs/sdqd_testqueue')

    job.status = 'running'
    job.executionStart = new Date(Date.now() - 10000)
    job.pid = 99999

    // First maintenance cycle — should kill
    await executor.maintainVisibility()
    expect(job.killed).toBe(true)
    expect(executor.stats.jobsKilled).toBe(1)

    // Second maintenance cycle — should NOT kill again
    await executor.maintainVisibility()
    expect(executor.stats.jobsKilled).toBe(1) // still 1
  })

  test('killAfter uses the exact deadline instead of waiting for maintenance', async () => {
    jest.useFakeTimers()
    const executor = makeExecutor({ killAfter: 1 })
    const killTree = jest.fn((pid, signal, callback) => callback?.())
    executor.opt.killTree = killTree

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
})
