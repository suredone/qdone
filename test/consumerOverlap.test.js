import { jest } from '@jest/globals'
import {
  ReceiveMessageCommand,
  GetQueueUrlCommand,
  ChangeMessageVisibilityBatchCommand,
  DeleteMessageBatchCommand
} from '@aws-sdk/client-sqs'
import { mockClient } from 'aws-sdk-client-mock'

// Neutralize latency/load/memory backpressure so scheduler behavior is
// deterministic and machine-independent — this test is about queue rotation,
// not backpressure.
jest.unstable_mockModule('os', () => {
  const os = {
    freemem: () => 16e9,
    totalmem: () => 16e9,
    cpus: () => new Array(4).fill({ model: 'mock' })
  }
  return { ...os, default: os }
})
jest.unstable_mockModule('../src/scheduler/systemMonitor.js', () => ({
  SystemMonitor: class {
    getLatency () { return 1 }
    getLoad () { return 0 }
    shutdown () {}
  }
}))

const { processMessages, requestShutdown } = await import('../src/consumer.js')
const { getSQSClient, setSQSClient } = await import('../src/sqs.js')
const { qrlCacheClear } = await import('../src/qrlCache.js')

const BASE_URL = 'https://sqs.us-east-1.amazonaws.com/123456/'

let sqsMock

afterAll(() => {
  sqsMock?.restore()
})

// A successful receive must release the queue for further receives while its
// batch is still executing. Holding the queue "listening" until its slowest
// job finishes would serialize a hot queue into batch lockstep.
test('receives on the same queue overlap with job execution', async () => {
  qrlCacheClear()
  const client = getSQSClient()
  sqsMock = mockClient(client)
  setSQSClient(sqsMock)
  sqsMock.on(ChangeMessageVisibilityBatchCommand).resolves({ Successful: [], Failed: [] })
  sqsMock.on(DeleteMessageBatchCommand).resolves({ Successful: [], Failed: [] })
  sqsMock.on(GetQueueUrlCommand).callsFake((input) => ({ QueueUrl: BASE_URL + input.QueueName }))

  let nextMessageId = 0
  sqsMock.on(ReceiveMessageCommand).callsFake(() => ({
    Messages: new Array(10).fill(null).map(() => ({
      MessageId: 'm-' + nextMessageId,
      ReceiptHandle: 'rh-' + nextMessageId++,
      Body: 'noop'
    }))
  }))

  // Every job blocks on the gate, so the first batch is still executing while
  // the scheduler decides whether to poll the queue again.
  let releaseJobs
  const gate = new Promise((resolve) => { releaseJobs = resolve })
  processMessages(['overlap'], () => gate, { maxConcurrentJobs: 100, disableLog: true })

  // The scheduler loop ticks every 300ms; give it several ticks.
  await new Promise((resolve) => setTimeout(resolve, 1200))
  const receives = sqsMock.commandCalls(ReceiveMessageCommand).length
  releaseJobs()
  await requestShutdown()
  await new Promise((resolve) => setTimeout(resolve, 600))

  expect(receives).toBeGreaterThanOrEqual(2)
}, 20000)
