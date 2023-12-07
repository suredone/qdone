import { ListQueuesCommand, GetQueueUrlCommand } from '@aws-sdk/client-sqs'
import { getClient, setClient } from '../src/sqs.js'
import {
  normalizeQueueName,
  normalizeFailQueueName,
  qrlCacheGet,
  qrlCacheSet,
  qrlCacheClear,
  qrlCacheInvalidate,
  ingestQRLs,
  getMatchingQueues,
  getQnameUrlPairs
} from '../src/qrlCache.js'
import { mockClient } from 'aws-sdk-client-mock'
import 'aws-sdk-client-mock-jest'

getClient()
const client = getClient()

// Always clear qrl cache at the beginning of each test
beforeEach(qrlCacheClear)

describe('normalizeQueueName', () => {
  test('names pass through when no options are set', () => {
    const options = {}
    expect(normalizeQueueName('testqueue', options)).toBe('testqueue')
  })
  test('names get .fifo appended when that option is set', () => {
    const options = { fifo: true }
    expect(normalizeQueueName('testqueue', options)).toBe('testqueue.fifo')
  })
  test('names named .fifo remain unchanged', () => {
    const options = { fifo: true }
    expect(normalizeQueueName('testqueue.fifo', options)).toBe('testqueue.fifo')
  })
})

describe('normalizeFailQueueName', () => {
  test('names get fail-suffix appended when no options are set', () => {
    const options = { 'fail-suffix': '_failed' }
    expect(normalizeFailQueueName('testqueue', options)).toBe('testqueue_failed')
  })
  test('names get .fifo appended when that option is set', () => {
    const options = { 'fail-suffix': '_failed', fifo: true }
    expect(normalizeFailQueueName('testqueue_failed', options)).toBe('testqueue_failed.fifo')
  })
  test('queues named .fifo get fail suffix appended', () => {
    const options = { 'fail-suffix': '_failed', fifo: true }
    expect(normalizeFailQueueName('testqueue.fifo', options)).toBe('testqueue_failed.fifo')
  })
})

describe('qrlCacheGet', () => {
  test('basic get works', async () => {
    const sqsMock = mockClient(client)
    setClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({
        QueueUrl: 'https://sqs.us-east-1.amazonaws.com/foobar/testqueue'
      })
    await expect(
      qrlCacheGet('testqueue')
    ).resolves.toEqual(
      'https://sqs.us-east-1.amazonaws.com/foobar/testqueue'
    )
    expect(sqsMock).toHaveReceivedCommand(GetQueueUrlCommand, {
      QueueName: 'testqueue'
    })
    expect(sqsMock).toHaveReceivedCommandTimes(GetQueueUrlCommand, 1)
  })

  test('repeated get does not call api', async () => {
    const sqsMock = mockClient(client)
    setClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({
        QueueUrl: 'https://sqs.us-east-1.amazonaws.com/foobar/testqueue'
      })
    await expect(
      qrlCacheGet('testqueue')
    ).resolves.toEqual(
      'https://sqs.us-east-1.amazonaws.com/foobar/testqueue'
    )
    await expect(
      qrlCacheGet('testqueue')
    ).resolves.toEqual(
      'https://sqs.us-east-1.amazonaws.com/foobar/testqueue'
    )
    expect(sqsMock).toHaveReceivedCommandTimes(GetQueueUrlCommand, 1)
  })
  // test('nonexistent queue throws exception', async () => {
  //   setClient(client)
  //   await expect(
  //     qrlCacheGet('foobar')
  //   ).resolves.toEqual(
  //     'https://sqs.us-east-1.amazonaws.com/foobar/foobar'
  //   )
  // })
})

describe('qrlCacheSet', () => {
  test('basic set works', async () => {
    const qname = 'testqueue'
    const qrl = 'https://sqs.us-east-1.amazonaws.com/foobar/testqueue'
    qrlCacheSet(qname, qrl)
    expect(await qrlCacheGet(qname)).toBe(qrl)
  })
})

describe('qrlCacheClear', () => {
  test('basic clear works', async () => {
    const sqsMock = mockClient(client)
    setClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({
        QueueUrl: 'https://sqs.us-east-1.amazonaws.com/foobar/foobar'
      })
    const qname = 'foobar'
    const qrl = 'https://sqs.us-east-1.amazonaws.com/foobar/foobar'
    qrlCacheClear()
    expect(await qrlCacheGet(qname)).toBe(qrl)
    expect(sqsMock).toHaveReceivedCommandTimes(GetQueueUrlCommand, 1)
  })
})

describe('qrlCacheInvalidate', () => {
  test('basic invalidate works', async () => {
    const sqsMock = mockClient(client)
    setClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand)
      .resolves({
        QueueUrl: 'https://sqs.us-east-1.amazonaws.com/foobar/foobar'
      })
    const qname = 'foobar'
    const qrl = 'https://sqs.us-east-1.amazonaws.com/foobar/foobar'
    expect(await qrlCacheGet(qname)).toBe(qrl)
    qrlCacheInvalidate(qname)
    expect(await qrlCacheGet(qname)).toBe(qrl)
    expect(sqsMock).toHaveReceivedCommandTimes(GetQueueUrlCommand, 2)
  })
})

describe('ingestQRLs', () => {
  test('ingests specified qrls and they are cached', async () => {
    const qname = 'foobar'
    const qrl = 'https://sqs.us-east-1.amazonaws.com/foobar/foobar'
    const qrls = [
      `${qrl}_1`,
      `${qrl}_2`,
      `${qrl}_3`
    ]
    const expectedResults = [
      { qname: `${qname}_1`, qrl: `${qrl}_1` },
      { qname: `${qname}_2`, qrl: `${qrl}_2` },
      { qname: `${qname}_3`, qrl: `${qrl}_3` }
    ]
    expect(ingestQRLs(qrls)).toEqual(expectedResults)
    expect(qrlCacheGet(expectedResults[0].qname)).resolves.toEqual(expectedResults[0].qrl)
    expect(qrlCacheGet(expectedResults[1].qname)).resolves.toEqual(expectedResults[1].qrl)
    expect(qrlCacheGet(expectedResults[2].qname)).resolves.toEqual(expectedResults[2].qrl)
  })
})

describe('getMatchingQueues', () => {
  test('gets prefixed queues when there is a single page', async () => {
    const sqsMock = mockClient(client)
    setClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand)
      .resolves({
        QueueUrl: 'https://sqs.us-east-1.amazonaws.com/foobar/foobar'
      })
      .on(ListQueuesCommand, { QueueNamePrefix: 'foobar' })
      .resolvesOnce({
        QueueUrls: [
          'https://sqs.us-east-1.amazonaws.com/foobar/foobar_1'
        ]
      })
    const qname = 'foobar'
    const qrl = 'https://sqs.us-east-1.amazonaws.com/foobar/foobar_1'
    expect(await getMatchingQueues(qname)).toEqual([qrl])
    expect(sqsMock).toHaveReceivedCommandTimes(ListQueuesCommand, 1)
  })

  test('gets matching queues when there are multiple pages', async () => {
    const sqsMock = mockClient(client)
    setClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand)
      .resolves({
        QueueUrl: 'https://sqs.us-east-1.amazonaws.com/foobar/foobar'
      })
      .on(ListQueuesCommand, {
        QueueNamePrefix: 'foobar'
      })
      .resolvesOnce({
        $metadata: {
          httpStatusCode: 200,
          requestId: '00000000-0000-0000-0000-000000000000',
          extendedRequestId: undefined,
          cfId: undefined,
          attempts: 1,
          totalRetryDelay: 0
        },
        QueueUrls: [
          'https://sqs.us-east-1.amazonaws.com/foobar/foobar_1',
          'https://sqs.us-east-1.amazonaws.com/foobar/foobar_2'
        ],
        NextToken: 'fizz'
      })
      .resolvesOnce({
        $metadata: {
          httpStatusCode: 200,
          requestId: '00000000-0000-0000-0000-000000000000',
          extendedRequestId: undefined,
          cfId: undefined,
          attempts: 1,
          totalRetryDelay: 0
        },
        QueueUrls: [
          'https://sqs.us-east-1.amazonaws.com/foobar/foobar_3',
          'https://sqs.us-east-1.amazonaws.com/foobar/foobar_4'
        ],
        NextToken: 'buzz'
      })
      .resolvesOnce({
        QueueUrls: [
          'https://sqs.us-east-1.amazonaws.com/foobar/foobar_5',
          'https://sqs.us-east-1.amazonaws.com/foobar/foobar_6'
        ]
      })
    const qname = 'foobar'
    const qrl = 'https://sqs.us-east-1.amazonaws.com/foobar/foobar'
    expect(await getMatchingQueues(qname)).toEqual([
      `${qrl}_1`,
      `${qrl}_2`,
      `${qrl}_3`,
      `${qrl}_4`,
      `${qrl}_5`,
      `${qrl}_6`
    ])
    expect(sqsMock).toHaveReceivedCommandTimes(ListQueuesCommand, 3)
  })
})

describe('getQnameUrlPairs', () => {
  test('basic pairs with no wildcard queues work', async () => {
    const sqsMock = mockClient(client)
    setClient(sqsMock)
    const qnames = []
    const expectedResult = []
    const N = 10

    // Setup paired requests, responses and mocks
    for (let i = 0; i < N; i++) {
      const qname = `foobar_${i}`
      const qrl = `$https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
      qnames.push(qname)
      expectedResult.push({ qname, qrl })
      sqsMock
        .on(GetQueueUrlCommand, { QueueName: qname })
        .resolves({ QueueUrl: qrl })
    }

    // Test command
    expect(await getQnameUrlPairs(qnames)).toEqual(expectedResult)
    expect(sqsMock).toHaveReceivedCommandTimes(GetQueueUrlCommand, N)
  })

  test('test pairs with mixed wildcard queues work', async () => {
    const sqsMock = mockClient(client)
    const wildname = 'foobar_wild_'
    const wildurl = `https://sqs.us-east-1.amazonaws.com/foobar/${wildname}`
    setClient(sqsMock)
    sqsMock
      .on(ListQueuesCommand, { QueueNamePrefix: wildname })
      .resolvesOnce({
        QueueUrls: [`${wildurl}1`, `${wildurl}2`],
        NextToken: 'fizz'
      })
      .resolvesOnce({
        QueueUrls: [`${wildurl}3`, `${wildurl}4`]
      })

    // Setup test data
    const N = 10
    const qnames = ['foobar_wild_*']
    const expectedResult = [
      { qname: `${wildname}1`, qrl: `${wildurl}1` },
      { qname: `${wildname}2`, qrl: `${wildurl}2` },
      { qname: `${wildname}3`, qrl: `${wildurl}3` },
      { qname: `${wildname}4`, qrl: `${wildurl}4` }
    ]

    // Setup paired requests, responses and mocks
    for (let i = 0; i < N; i++) {
      const qname = `foobar_${i + 1}`
      const qrl = `$https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
      qnames.push(qname)
      expectedResult.push({ qname, qrl })
      sqsMock
        .on(GetQueueUrlCommand, { QueueName: qname })
        .resolves({ QueueUrl: qrl })
    }

    // Test command
    const options = {}
    expect(await getQnameUrlPairs(qnames, options)).toEqual(expectedResult)
    expect(sqsMock).toHaveReceivedCommandTimes(ListQueuesCommand, 2)
    expect(sqsMock).toHaveReceivedCommandTimes(GetQueueUrlCommand, N)
  })

  test('test fifo pairs with mixed wildcard queues only returns fifo', async () => {
    const sqsMock = mockClient(client)
    const wildname = 'foobar_wild_'
    const wildurl = `https://sqs.us-east-1.amazonaws.com/foobar/${wildname}`
    setClient(sqsMock)
    sqsMock
      .on(ListQueuesCommand, { QueueNamePrefix: wildname })
      .resolvesOnce({
        QueueUrls: [`${wildurl}1`, `${wildurl}2.fifo`],
        NextToken: 'fizz'
      })
      .resolvesOnce({
        QueueUrls: [`${wildurl}3`, `${wildurl}4.fifo`],
        NextToken: 'buzz'
      })
      .resolvesOnce({
        QueueUrls: [`${wildurl}5`, `${wildurl}6.fifo`]
      })

    // Setup test data
    const N = 10
    const qnames = ['foobar_wild_*', 'nonexistent_queue_*']
    const expectedResult = [
      { qname: `${wildname}2.fifo`, qrl: `${wildurl}2.fifo` },
      { qname: `${wildname}4.fifo`, qrl: `${wildurl}4.fifo` },
      { qname: `${wildname}6.fifo`, qrl: `${wildurl}6.fifo` }
    ]

    // Setup paired requests, responses and mocks
    for (let i = 0; i < N; i++) {
      const qname = `foobar_${i + 1}`
      const qrl = `$https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
      qnames.push(qname)
      expectedResult.push({ qname, qrl })
      sqsMock
        .on(GetQueueUrlCommand, { QueueName: qname })
        .resolves({ QueueUrl: qrl })
    }

    // Test command
    const options = { fifo: true }
    expect(await getQnameUrlPairs(qnames, options)).toEqual(expectedResult)
    expect(sqsMock).toHaveReceivedCommandTimes(ListQueuesCommand, 4)
    expect(sqsMock).toHaveReceivedCommandTimes(GetQueueUrlCommand, N)
  })
})
