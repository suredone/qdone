import { GetMetricStatisticsCommand } from '@aws-sdk/client-cloudwatch'
import { GetQueueAttributesCommand, DeleteQueueCommand } from '@aws-sdk/client-sqs'
import { getClient as getSQSClient, setClient as setSQSClient } from '../src/sqs.js'
import { getCloudWatchClient, setCloudWatchClient } from '../src/cloudWatch.js'
import {
  attributeNames,
  _cheapIdleCheck,
  cheapIdleCheck,
  getMetric
} from '../src/idleQueues.js'
import { mockClient } from 'aws-sdk-client-mock'
import 'aws-sdk-client-mock-jest'

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

describe('cheapIdleCheck', () => {
  test('makes one api call with cache-uri not set', async () => {
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
        GetQueueAttributesCommand,
        {
          Dimensions: [{ Name: 'QueueName', Value: qname }],
          MetricName: metricName
        }
      )
  })
})
