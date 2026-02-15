import { GetMetricStatisticsCommand } from '@aws-sdk/client-cloudwatch'
import { GetQueueAttributesCommand, DeleteQueueCommand, QueueDoesNotExist } from '@aws-sdk/client-sqs'
import { getSQSClient, setSQSClient } from '../src/sqs.js'
import { getOptionsWithDefaults } from '../src/defaults.js'
import { getCloudWatchClient, setCloudWatchClient } from '../src/cloudWatch.js'
import {
  attributeNames,
  _cheapIdleCheck,
  cheapIdleCheck,
  getMetric,
  checkIdle,
  deleteQueue,
  processQueueSet,
  idleQueues,
  stripSuffixes
} from '../src/idleQueues.js'
import { mockClient } from 'aws-sdk-client-mock'
import 'aws-sdk-client-mock-jest'

import Redis from 'ioredis-mock'
import { setCache, getCacheClient, shutdownCache } from '../src/cache.js'
import { qrlCacheSet } from '../src/qrlCache.js'

// Redis setup
const cacheOpt = {
  cacheUri: 'redis://localhost',
  cacheTtlSeconds: 10,
  cachePrefix: 'qdone:',
  Redis
}
beforeEach(shutdownCache)
afterEach(async () => getCacheClient(cacheOpt).flushall())
afterAll(shutdownCache)

// AWS setup
getSQSClient()
const sqsClient = getSQSClient()
getCloudWatchClient()
const cloudWatchClient = getCloudWatchClient()

// Always clear qrl cache at the beginning of each test
// beforeEach(qrlCacheClear)

describe('_cheapIdleCheck', () => {
  test('makes proper api call', async () => {
    const options = { prefix: '' }
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const sqsMock = mockClient(sqsClient)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueAttributesCommand)
      .resolvesOnce({
        QueueUrl: qrl,
        Attributes: {
          ApproximateNumberOfMessages: '1',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    await expect(
      _cheapIdleCheck(qname, qrl, options)
    ).resolves.toEqual({
      SQS: 1,
      result: {
        ApproximateNumberOfMessages: '1',
        ApproximateNumberOfMessagesNotVisible: '0',
        idle: false,
        exists: true,
        queue: qname
      }
    })
    expect(sqsMock)
      .toHaveReceivedNthCommandWith(
        1,
        GetQueueAttributesCommand,
        { QueueUrl: qrl, AttributeNames: attributeNames }
      )
  })

  test('handles nonexistent queues properly', async () => {
    const options = { prefix: '' }
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const sqsMock = mockClient(sqsClient)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueAttributesCommand)
      .rejectsOnce(new QueueDoesNotExist())
    await expect(
      _cheapIdleCheck(qname, qrl, options)
    ).resolves.toEqual({
      SQS: 1,
      result: {
        idle: undefined,
        exists: false
      }
    })
    expect(sqsMock)
      .toHaveReceivedNthCommandWith(
        1,
        GetQueueAttributesCommand,
        { QueueUrl: qrl, AttributeNames: attributeNames }
      )
  })

  test('rethrows all other exceptions it does not understand', async () => {
    const options = { prefix: '' }
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const sqsMock = mockClient(sqsClient)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueAttributesCommand)
      .rejectsOnce(new Error('humbug'))
    await expect(
      _cheapIdleCheck(qname, qrl, options)
    ).rejects.toThrow('humbug')
    expect(sqsMock)
      .toHaveReceivedNthCommandWith(
        1,
        GetQueueAttributesCommand,
        { QueueUrl: qrl, AttributeNames: attributeNames }
      )
  })
})

describe('cheapIdleCheck', () => {
  test('makes one api call with cacheUri not set', async () => {
    const options = { prefix: '' }
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const sqsMock = mockClient(sqsClient)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueAttributesCommand)
      .resolvesOnce({
        QueueUrl: qrl,
        Attributes: {
          ApproximateNumberOfMessages: '1',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    await expect(
      cheapIdleCheck(qname, qrl, options)
    ).resolves.toEqual({
      SQS: 1,
      result: {
        ApproximateNumberOfMessages: '1',
        ApproximateNumberOfMessagesNotVisible: '0',
        idle: false,
        exists: true,
        queue: qname
      }
    })
    expect(sqsMock)
      .toHaveReceivedNthCommandWith(
        1,
        GetQueueAttributesCommand,
        { QueueUrl: qrl, AttributeNames: attributeNames }
      )
  })

  test('checks the cache if it has one', async () => {
    const opt = { prefix: '', ...cacheOpt }
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const sqsMock = mockClient(sqsClient)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueAttributesCommand)
      .resolvesOnce({
        QueueUrl: qrl,
        Attributes: {
          ApproximateNumberOfMessages: '1',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    await setCache(
      'cheap-idle-check:' + qrl,
      {
        ApproximateNumberOfMessages: '1',
        ApproximateNumberOfMessagesNotVisible: '0',
        idle: false,
        exists: true,
        queue: qname
      },
      opt
    )
    await expect(
      cheapIdleCheck(qname, qrl, opt)
    ).resolves.toEqual({
      SQS: 0,
      result: {
        ApproximateNumberOfMessages: '1',
        ApproximateNumberOfMessagesNotVisible: '0',
        idle: false,
        exists: true,
        queue: qname
      }
    })
    expect(sqsMock)
      .not.toHaveReceivedNthCommandWith(
        1,
        GetQueueAttributesCommand,
        { QueueUrl: qrl, AttributeNames: attributeNames }
      )
  })

  test('checks calls api on a cache miss', async () => {
    const opt = { prefix: '', ...cacheOpt }
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const sqsMock = mockClient(sqsClient)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueAttributesCommand)
      .resolvesOnce({
        QueueUrl: qrl,
        Attributes: {
          ApproximateNumberOfMessages: '1',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    await expect(
      cheapIdleCheck(qname, qrl, opt)
    ).resolves.toEqual({
      SQS: 1,
      result: {
        ApproximateNumberOfMessages: '1',
        ApproximateNumberOfMessagesNotVisible: '0',
        idle: false,
        exists: true,
        queue: qname
      }
    })
    expect(sqsMock)
      .toHaveReceivedNthCommandWith(
        1,
        GetQueueAttributesCommand,
        { QueueUrl: qrl, AttributeNames: attributeNames }
      )
  })
})

describe('getMetric', () => {
  test('makes one api call', async () => {
    const options = { prefix: '', 'idle-for': 60 }
    const metricName = 'TestMetric'
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const cloudWatcHMock = mockClient(cloudWatchClient)
    setCloudWatchClient(cloudWatcHMock)
    cloudWatcHMock
      .on(GetMetricStatisticsCommand)
      .resolvesOnce({
        Datapoints: [
          { Sum: 1 },
          { Sum: 1 }
        ]
      })
    await expect(
      getMetric(qname, qrl, metricName, options)
    ).resolves.toEqual({ [metricName]: 2 })
    expect(cloudWatcHMock)
      .toHaveReceivedNthCommandWith(
        1,
        GetMetricStatisticsCommand,
        {
          Dimensions: [{ Name: 'QueueName', Value: qname }],
          MetricName: metricName
        }
      )
  })
})

describe('checkIdle', () => {
  test('gets short circuited when cheapIdleCheck is conclusive', async () => {
    const options = { prefix: '' }
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const sqsMock = mockClient(sqsClient)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueAttributesCommand)
      .resolvesOnce({
        QueueUrl: qrl,
        Attributes: {
          ApproximateNumberOfMessages: '1',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    await expect(
      checkIdle(qname, qrl, options)
    ).resolves.toEqual({
      queue: qname,
      cheap: {
        SQS: 1,
        result: {
          ApproximateNumberOfMessages: '1',
          ApproximateNumberOfMessagesNotVisible: '0',
          idle: false,
          exists: true,
          queue: qname
        }
      },
      idle: false,
      exists: true,
      apiCalls: {
        SQS: 1,
        CloudWatch: 0
      }
    })
    expect(sqsMock)
      .toHaveReceivedNthCommandWith(
        1,
        GetQueueAttributesCommand,
        { QueueUrl: qrl, AttributeNames: attributeNames }
      )
  })

  test('gets short circuited when cheapIdleCheck is conclusive', async () => {
    const options = { prefix: '' }
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const sqsMock = mockClient(sqsClient)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueAttributesCommand)
      .resolvesOnce({
        QueueUrl: qrl,
        Attributes: {
          ApproximateNumberOfMessages: '1',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    await expect(
      checkIdle(qname, qrl, options)
    ).resolves.toEqual({
      queue: qname,
      cheap: {
        SQS: 1,
        result: {
          ApproximateNumberOfMessages: '1',
          ApproximateNumberOfMessagesNotVisible: '0',
          idle: false,
          exists: true,
          queue: qname
        }
      },
      idle: false,
      exists: true,
      apiCalls: {
        SQS: 1,
        CloudWatch: 0
      }
    })
    expect(sqsMock)
      .toHaveReceivedNthCommandWith(
        1,
        GetQueueAttributesCommand,
        { QueueUrl: qrl, AttributeNames: attributeNames }
      )
  })
})

describe('deleteQueue', () => {
  test('makes one api call', async () => {
    const opt = getOptionsWithDefaults({ prefix: '' })
    const sqsMock = mockClient(sqsClient)
    setSQSClient(sqsMock)
    sqsMock
      .on(DeleteQueueCommand)
      .resolvesOnce({})
    await expect(deleteQueue('test', 'https://sqs.us-east-1.amazonaws.com/example/test', opt))
      .resolves.toEqual({
        deleted: true,
        apiCalls: { SQS: 1, CloudWatch: 0 }
      })
  })
})

describe('processQueueSet', () => {
  test('completes execution', async () => {
    const options = { prefix: '' }
    const opt = getOptionsWithDefaults(options)
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const sqsMock = mockClient(sqsClient)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueAttributesCommand)
      .resolves({
        QueueUrl: qrl,
        Attributes: {
          ApproximateNumberOfMessages: '1',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    await processQueueSet('test', 'https://sqs.us-east-1.amazonaws.com/example/test', opt)
  })
})

describe('stripSuffixes', () => {
  test('works for all examples', () => {
    const opt = getOptionsWithDefaults()
    expect(stripSuffixes('asdf', opt)).toBe('asdf')
    expect(stripSuffixes('asdf.fifo', opt)).toBe('asdf.fifo')
    expect(stripSuffixes('asdf_failed', opt)).toBe('asdf')
    expect(stripSuffixes('asdf_failed.fifo', opt)).toBe('asdf.fifo')
    expect(stripSuffixes('asdf_dead', opt)).toBe('asdf')
    expect(stripSuffixes('asdf_dead.fifo', opt)).toBe('asdf.fifo')
  })
})

describe('idleQueues', () => {
  test('enpty queues returns empty result', async () => {
    await expect(idleQueues([], { prefix: '' })).resolves.toEqual('noQueues')
  })

  test('single queue returns expected result', async () => {
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const sqsMock = mockClient(sqsClient)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueAttributesCommand)
      .resolves({
        QueueUrl: qrl,
        Attributes: {
          ApproximateNumberOfMessages: '1',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    await qrlCacheSet('test', 'https://sqs.us-east-1.amazonaws.com/example/test')
    await expect(idleQueues(['test'], { prefix: '' })).resolves.toEqual([{
      apiCalls: { SQS: 3, CloudWatch: 0 },
      idle: false,
      queue: 'test'
    }])
  })
})
