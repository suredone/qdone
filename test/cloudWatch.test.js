import { getCloudWatchClient, setCloudWatchClient, putAggregateData } from '../src/cloudWatch.js'
import { PutMetricDataCommand } from '@aws-sdk/client-cloudwatch'
import { mockClient } from 'aws-sdk-client-mock'
import 'aws-sdk-client-mock-jest'

describe('putData', () => {
  getCloudWatchClient()
  const client = getCloudWatchClient()

  test('pushes expected data to CloudWatch', async () => {
    const cwMock = mockClient(client)
    setCloudWatchClient(cwMock)
    cwMock
      .on(PutMetricDataCommand)
      .resolvesOnce({
        $metadata: {
          httpStatusCode: 200,
          requestId: '00000000-0000-0000-0000-000000000000',
          extendedRequestId: undefined,
          cfId: undefined,
          attempts: 1,
          totalRetryDelay: 0
        }
      })
    const queueName = 'sdqd_amzn_orders_*_failed'
    const data = {
      queueName,
      totalQueues: 409,
      contributingQueueNames: ['one', 'two', 'three'],
      ApproximateNumberOfMessages: 100,
      ApproximateNumberOfMessagesDelayed: 11,
      ApproximateNumberOfMessagesNotVisible: 2
    }
    const now = new Date()
    await expect(putAggregateData(data, now)).resolves.toEqual()
    await expect(putAggregateData(data)).resolves.toEqual()
    // Force the zero path for more coverage
    delete data.ApproximateNumberOfMessages
    delete data.ApproximateNumberOfMessagesDelayed
    delete data.ApproximateNumberOfMessagesNotVisible
    await expect(putAggregateData(data, now)).resolves.toEqual()
    expect(cwMock)
      .toHaveReceivedNthSpecificCommandWith(1, PutMetricDataCommand, {
        Namespace: 'qmonitor',
        MetricData: [
          {
            MetricName: 'totalQueues',
            Dimensions: [{ Name: 'queueName', Value: queueName }],
            Timestamp: now,
            Value: 409,
            Unit: 'Count'
          },
          {
            MetricName: 'contributingQueueCount',
            Dimensions: [{ Name: 'queueName', Value: queueName }],
            Timestamp: now,
            Value: 3,
            Unit: 'Count'
          },
          {
            MetricName: 'ApproximateNumberOfMessages',
            Dimensions: [{ Name: 'queueName', Value: queueName }],
            Timestamp: now,
            Value: 100,
            Unit: 'Count'
          },
          {
            MetricName: 'ApproximateNumberOfMessagesDelayed',
            Dimensions: [{ Name: 'queueName', Value: queueName }],
            Timestamp: now,
            Value: 11,
            Unit: 'Count'
          },
          {
            MetricName: 'ApproximateNumberOfMessagesNotVisible',
            Dimensions: [{ Name: 'queueName', Value: queueName }],
            Timestamp: now,
            Value: 2,
            Unit: 'Count'
          }
        ]
      })
    expect(cwMock)
      .toHaveReceivedNthSpecificCommandWith(3, PutMetricDataCommand, {
        Namespace: 'qmonitor',
        MetricData: [
          {
            MetricName: 'totalQueues',
            Dimensions: [{ Name: 'queueName', Value: queueName }],
            Timestamp: now,
            Value: 409,
            Unit: 'Count'
          },
          {
            MetricName: 'contributingQueueCount',
            Dimensions: [{ Name: 'queueName', Value: queueName }],
            Timestamp: now,
            Value: 3,
            Unit: 'Count'
          },
          {
            MetricName: 'ApproximateNumberOfMessages',
            Dimensions: [{ Name: 'queueName', Value: queueName }],
            Timestamp: now,
            Value: 0,
            Unit: 'Count'
          },
          {
            MetricName: 'ApproximateNumberOfMessagesDelayed',
            Dimensions: [{ Name: 'queueName', Value: queueName }],
            Timestamp: now,
            Value: 0,
            Unit: 'Count'
          },
          {
            MetricName: 'ApproximateNumberOfMessagesNotVisible',
            Dimensions: [{ Name: 'queueName', Value: queueName }],
            Timestamp: now,
            Value: 0,
            Unit: 'Count'
          }
        ]
      })
  })
})
