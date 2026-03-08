import { interpretWildcard, getAggregateData, getQueueAge } from '../src/monitor.js'
import { getSQSClient, setSQSClient } from '../src/sqs.js'
import { getCloudWatchClient, setCloudWatchClient } from '../src/cloudWatch.js'
import { ListQueuesCommand, GetQueueAttributesCommand } from '@aws-sdk/client-sqs'
import { GetMetricStatisticsCommand } from '@aws-sdk/client-cloudwatch'
import { mockClient } from 'aws-sdk-client-mock'
import 'aws-sdk-client-mock-jest'

describe('interpretWildcard', () => {
  test('wildcard splits into prefix and suffix', () => {
    expect(
      interpretWildcard('test*case')
    )
      .toEqual({
        prefix: 'test',
        suffix: 'case',
        safeSuffix: 'case',
        suffixRegex: /case$/
      })
  })

  test('double wildcard behaves sensibly', () => {
    expect(
      interpretWildcard('test**case')
    )
      .toEqual({
        prefix: 'test',
        suffix: '',
        safeSuffix: '',
        suffixRegex: /$/
      })
  })

  test('absent wildcard behaves sensibly', () => {
    expect(
      interpretWildcard('test')
    )
      .toEqual({
        prefix: 'test',
        suffix: undefined,
        safeSuffix: '',
        suffixRegex: /$/
      })
  })

  test('fifo option appends suffix', () => {
    expect(
      interpretWildcard('test_*_case.fifo')
    )
      .toEqual({
        prefix: 'test_',
        suffix: '_case.fifo',
        safeSuffix: '_case\\.fifo',
        suffixRegex: /_case\.fifo$/
      })
  })
})

describe('getQueueAge', () => {
  getCloudWatchClient()
  const cwClient = getCloudWatchClient()

  test('returns max from datapoints', async () => {
    const cwMock = mockClient(cwClient)
    setCloudWatchClient(cwMock)
    cwMock
      .on(GetMetricStatisticsCommand)
      .resolvesOnce({
        Datapoints: [
          { Maximum: 300 },
          { Maximum: 600 }
        ]
      })
    await expect(getQueueAge('test_queue')).resolves.toBe(600)
    expect(cwMock)
      .toHaveReceivedCommandWith(GetMetricStatisticsCommand, {
        MetricName: 'ApproximateAgeOfOldestMessage',
        Namespace: 'AWS/SQS',
        Period: 300,
        Dimensions: [{ Name: 'QueueName', Value: 'test_queue' }],
        Statistics: ['Maximum']
      })
  })

  test('returns 0 when no datapoints', async () => {
    const cwMock = mockClient(cwClient)
    setCloudWatchClient(cwMock)
    cwMock
      .on(GetMetricStatisticsCommand)
      .resolvesOnce({ Datapoints: [] })
    await expect(getQueueAge('test_queue')).resolves.toBe(0)
  })

  test('returns 0 on CloudWatch error', async () => {
    const cwMock = mockClient(cwClient)
    setCloudWatchClient(cwMock)
    cwMock
      .on(GetMetricStatisticsCommand)
      .rejectsOnce(new Error('ThrottlingException'))
    await expect(getQueueAge('test_queue')).resolves.toBe(0)
  })
})

describe('getAggregateData', () => {
  getSQSClient()
  const sqsClient = getSQSClient()
  getCloudWatchClient()
  const cwClient = getCloudWatchClient()

  test('correctly aggregates multiple queues', async () => {
    const sqsMock = mockClient(sqsClient)
    setSQSClient(sqsMock)
    const cwMock = mockClient(cwClient)
    setCloudWatchClient(cwMock)
    sqsMock
      .on(ListQueuesCommand)
      .resolvesOnce({
        QueueUrls: [
          'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_amzn_orders_0_1021',
          'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_amzn_orders_0_1021_failed',
          'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_amzn_orders_0_1022',
          'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_amzn_orders_0_1022_failed',
          'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_amzn_orders_0_1023',
          'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_amzn_orders_0_1023_failed'
        ]
      })
      .on(GetQueueAttributesCommand)
      .resolvesOnce({
        Attributes: {
          ApproximateNumberOfMessages: 10,
          ApproximateNumberOfMessagesDelayed: 1
        }
      })
      .resolvesOnce({
        Attributes: {
          ApproximateNumberOfMessages: 11,
          ApproximateNumberOfMessagesNotVisible: 0
        }
      })
      .resolvesOnce({
        Attributes: {
          ApproximateNumberOfMessagesDelayed: 2,
          ApproximateNumberOfMessagesNotVisible: 2
        }
      })
    // When the pattern itself targets _failed queues, age is still computed
    cwMock
      .on(GetMetricStatisticsCommand)
      .resolvesOnce({ Datapoints: [{ Maximum: 300 }] })
      .resolvesOnce({ Datapoints: [{ Maximum: 600 }] })
      .resolvesOnce({ Datapoints: [{ Maximum: 150 }] })
    const queueName = 'sdqd_amzn_orders_*_failed'
    await expect(
      getAggregateData(queueName)
    ).resolves.toEqual({
      queueName,
      totalQueues: 3,
      contributingQueueNames: [
        'sdqd_amzn_orders_0_1021_failed',
        'sdqd_amzn_orders_0_1022_failed',
        'sdqd_amzn_orders_0_1023_failed'
      ],
      ApproximateNumberOfMessages: 21,
      ApproximateNumberOfMessagesDelayed: 3,
      ApproximateNumberOfMessagesNotVisible: 2,
      ApproximateAgeOfOldestMessage: 600
    })
    expect(cwMock)
      .toHaveReceivedNthSpecificCommandWith(1, GetMetricStatisticsCommand, {
        MetricName: 'ApproximateAgeOfOldestMessage',
        Namespace: 'AWS/SQS',
        Dimensions: [{ Name: 'QueueName', Value: 'sdqd_amzn_orders_0_1021_failed' }]
      })
    expect(sqsMock)
      .toHaveReceivedNthSpecificCommandWith(1, ListQueuesCommand, {
        QueueNamePrefix: 'sdqd_amzn_orders_',
        MaxResults: 1000
      })
    expect(sqsMock)
      .toHaveReceivedNthSpecificCommandWith(1, GetQueueAttributesCommand, {
        QueueUrl: 'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_amzn_orders_0_1021_failed',
        AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible', 'ApproximateNumberOfMessagesDelayed']
      })
    expect(sqsMock)
      .toHaveReceivedNthSpecificCommandWith(2, GetQueueAttributesCommand, {
        QueueUrl: 'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_amzn_orders_0_1022_failed',
        AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible', 'ApproximateNumberOfMessagesDelayed']
      })
  })

  test('excludes dead and failed queues from age calculation', async () => {
    const sqsMock = mockClient(sqsClient)
    setSQSClient(sqsMock)
    const cwMock = mockClient(cwClient)
    setCloudWatchClient(cwMock)
    sqsMock
      .on(ListQueuesCommand)
      .resolvesOnce({
        QueueUrls: [
          'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_bulk_channel_123.fifo',
          'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_bulk_channel_123_failed.fifo',
          'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_bulk_channel_123_dead.fifo',
          'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_bulk_channel_456.fifo'
        ]
      })
      .on(GetQueueAttributesCommand)
      .resolvesOnce({ Attributes: { ApproximateNumberOfMessages: '5' } }) // active 123
      .resolvesOnce({ Attributes: { ApproximateNumberOfMessages: '2' } }) // failed 123
      .resolvesOnce({ Attributes: { ApproximateNumberOfMessages: '1' } }) // dead 123
      .resolvesOnce({ Attributes: { ApproximateNumberOfMessages: '3' } }) // active 456
    cwMock
      .on(GetMetricStatisticsCommand)
      .resolvesOnce({ Datapoints: [{ Maximum: 120 }] }) // active 123: 2 min
      .resolvesOnce({ Datapoints: [{ Maximum: 60 }] }) // active 456: 1 min

    const opt = { failSuffix: '_failed', dlqSuffix: '_dead' }
    const result = await getAggregateData('sdqd_bulk_channel_*.fifo', opt)

    // Age should be max of active queues only (120), NOT the dead queue age
    expect(result.ApproximateAgeOfOldestMessage).toBe(120)

    // Contributing queues should still include all queues (for depth metrics)
    expect(result.contributingQueueNames).toEqual(expect.arrayContaining([
      'sdqd_bulk_channel_123.fifo',
      'sdqd_bulk_channel_123_failed.fifo',
      'sdqd_bulk_channel_123_dead.fifo',
      'sdqd_bulk_channel_456.fifo'
    ]))

    // CloudWatch should only be called for active queues (2 calls, not 4)
    expect(cwMock).toHaveReceivedCommandTimes(GetMetricStatisticsCommand, 2)
    expect(cwMock).toHaveReceivedNthSpecificCommandWith(1, GetMetricStatisticsCommand, {
      Dimensions: [{ Name: 'QueueName', Value: 'sdqd_bulk_channel_123.fifo' }]
    })
    expect(cwMock).toHaveReceivedNthSpecificCommandWith(2, GetMetricStatisticsCommand, {
      Dimensions: [{ Name: 'QueueName', Value: 'sdqd_bulk_channel_456.fifo' }]
    })

    // Depth metrics should still aggregate all queues
    expect(result.ApproximateNumberOfMessages).toBe(11)
    expect(result.totalQueues).toBe(4)
  })

  test('excludes dead and failed queues from age for non-fifo queues', async () => {
    const sqsMock = mockClient(sqsClient)
    setSQSClient(sqsMock)
    const cwMock = mockClient(cwClient)
    setCloudWatchClient(cwMock)
    sqsMock
      .on(ListQueuesCommand)
      .resolvesOnce({
        QueueUrls: [
          'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_test_queue_1',
          'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_test_queue_1_failed',
          'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_test_queue_1_dead'
        ]
      })
      .on(GetQueueAttributesCommand)
      .resolvesOnce({ Attributes: { ApproximateNumberOfMessages: '5' } })
      .resolvesOnce({ Attributes: { ApproximateNumberOfMessages: '2' } })
      .resolvesOnce({ Attributes: { ApproximateNumberOfMessages: '1' } })
    cwMock
      .on(GetMetricStatisticsCommand)
      .resolvesOnce({ Datapoints: [{ Maximum: 90 }] })

    const opt = { failSuffix: '_failed', dlqSuffix: '_dead' }
    const result = await getAggregateData('sdqd_test_queue_*', opt)

    expect(result.ApproximateAgeOfOldestMessage).toBe(90)
    expect(cwMock).toHaveReceivedCommandTimes(GetMetricStatisticsCommand, 1)
    expect(result.totalQueues).toBe(3)
    expect(result.ApproximateNumberOfMessages).toBe(8)
  })
})
