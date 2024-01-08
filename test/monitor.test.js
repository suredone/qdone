import { interpretWildcard, getAggregateData } from '../src/monitor.js'
import { getSQSClient, setSQSClient } from '../src/sqs.js'
import { ListQueuesCommand, GetQueueAttributesCommand } from '@aws-sdk/client-sqs'
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

describe('getAggregateData', () => {
  getSQSClient()
  const client = getSQSClient()

  test('correctly aggregates multiple queues', async () => {
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
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
      ApproximateNumberOfMessagesNotVisible: 2
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
})
