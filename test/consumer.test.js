import { jest } from '@jest/globals'
import {
  ReceiveMessageCommand,
  GetQueueUrlCommand,
  QueueDoesNotExist,
  ChangeMessageVisibilityBatchCommand,
  DeleteMessageBatchCommand
} from '@aws-sdk/client-sqs'
import { mockClient } from 'aws-sdk-client-mock'

// The scheduler throttles how many receives it issues based on live event-loop
// latency, system load and free memory. On a busy host those factors pin the
// throughput to zero, so we neutralize them to make the receive behavior
// deterministic and machine-independent — this test is about accounting, not
// backpressure.
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
const N_GEN = 15
const N_QDNE = 15

let sqsMock
let result

// One shared run of the scheduler drives every assertion below. Every receive
// fails: half with a generic error, half with QueueDoesNotExist (the
// idle-queue-GC case). Both must release their concurrency accounting so the
// queues stay in rotation instead of the worker going permanently deaf.
async function runScenario () {
  qrlCacheClear()
  const client = getSQSClient()
  sqsMock = mockClient(client)
  setSQSClient(sqsMock)
  sqsMock.on(ChangeMessageVisibilityBatchCommand).resolves({ Successful: [], Failed: [] })
  sqsMock.on(DeleteMessageBatchCommand).resolves({ Successful: [], Failed: [] })
  sqsMock.on(GetQueueUrlCommand).callsFake((input) => ({ QueueUrl: BASE_URL + input.QueueName }))
  sqsMock.on(ReceiveMessageCommand).callsFake((input) => {
    if (input.QueueUrl.includes('_qdne_')) throw new QueueDoesNotExist({ $metadata: {}, message: 'gone' })
    throw new Error('simulated receive failure')
  })

  const names = []
  for (let i = 0; i < N_GEN; i++) names.push('gen_' + i)
  for (let i = 0; i < N_QDNE; i++) names.push('qdne_' + i)

  // Fire-and-forget; the loop only exits on requestShutdown.
  processMessages(names, async () => {}, { maxConcurrentJobs: 100, disableLog: true })
  await new Promise((resolve) => setTimeout(resolve, 2500))
  await requestShutdown()
  await new Promise((resolve) => setTimeout(resolve, 600))

  const urls = sqsMock.commandCalls(ReceiveMessageCommand).map((c) => c.args[0].input.QueueUrl)
  return {
    total: urls.length,
    distinct: new Set(urls).size,
    distinctGen: new Set(urls.filter((u) => u.includes('_gen_'))).size,
    distinctQdne: new Set(urls.filter((u) => u.includes('_qdne_'))).size
  }
}

afterAll(() => {
  sqsMock?.restore()
})

describe('processMessages receive-error accounting', () => {
  // Runs the shared scenario. A buggy rethrow out of the fire-and-forget
  // listen() would surface as an unhandled rejection during this test body,
  // which jest fails the test on — that is the assertion for this case.
  test('a receive error never surfaces as an unhandled rejection', async () => {
    result = await runScenario()
    expect(result.total).toBeGreaterThan(0) // sanity: the scenario actually polled
  }, 20000)

  test('a receive error keeps the queue in rotation and does not leak concurrency', () => {
    // If listeningQrls / maxReturnCount leaked, allowedJobs would pin to 0 and
    // polling would halt after ~10 receives; the fix keeps every queue pollable.
    expect(result.distinct).toBeGreaterThanOrEqual(20)
    expect(result.distinctGen).toBeGreaterThanOrEqual(10)
  })

  test('QueueDoesNotExist releases accounting too', () => {
    // The old catch backed off on QueueDoesNotExist but still leaked both
    // counters; every deleted queue must now be pollable again.
    expect(result.distinctQdne).toBeGreaterThanOrEqual(10)
  })
})
