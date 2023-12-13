import { ListQueuesCommand, GetQueueAttributesCommand } from '@aws-sdk/client-sqs'
import { getSQSClient, setSQSClient, getMatchingQueues, getQueueAttributes } from '../src/sqs.js'
import { mockClient } from 'aws-sdk-client-mock'
import 'aws-sdk-client-mock-jest'

describe('getMatchingQueues', () => {
  getSQSClient()
  const client = getSQSClient()

  test('regex /_failed$/ returns only failed queues', async () => {
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock
      .resolvesOnce({
        QueueUrls: [
          'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_amzn_orders_0_1021',
          'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_amzn_orders_0_1021_failed',
          'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_amzn_orders_0_1022',
          'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_amzn_orders_0_1022_failed'
        ]
      })
    await expect(
      getMatchingQueues('sdqd_amzn_orders_0', /_failed$/)
    )
      .resolves.toEqual([
        'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_amzn_orders_0_1021_failed',
        'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_amzn_orders_0_1022_failed'
      ])
    expect(sqsMock).toHaveReceivedCommand(ListQueuesCommand, {
      QueueNamePrefix: 'sdqd_amzn_orders_0',
      MaxResults: 1000
    })
  })

  test('follows next tokens', async () => {
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock
      .on(ListQueuesCommand, {
        QueueNamePrefix: 'sdqd_amzn_orders_1'
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
          'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_amzn_orders_1_1021',
          'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_amzn_orders_1_1021_failed'
        ],
        NextToken: 'foobar'
      })
      .resolvesOnce({
        QueueUrls: [
          'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_amzn_orders_1_1022',
          'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_amzn_orders_1_1022_failed'
        ]
      })
    await expect(
      getMatchingQueues('sdqd_amzn_orders_1', /_failed$/)
    )
      .resolves.toEqual([
        'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_amzn_orders_1_1021_failed',
        'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_amzn_orders_1_1022_failed'
      ])
    expect(sqsMock)
      .toHaveReceivedNthSpecificCommandWith(1, ListQueuesCommand, {
        QueueNamePrefix: 'sdqd_amzn_orders_1',
        MaxResults: 1000
      })
    expect(sqsMock)
      .toHaveReceivedNthSpecificCommandWith(2, ListQueuesCommand, {
        QueueNamePrefix: 'sdqd_amzn_orders_1',
        MaxResults: 1000,
        NextToken: 'foobar'
      })
  })

  test('handles blank returns', async () => {
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock
      .on(ListQueuesCommand, {
        QueueNamePrefix: 'sdqd_amzn_orders_1'
      })
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
    await expect(
      getMatchingQueues('sdqd_amzn_orders_1', /_failed$/)
    )
      .resolves.toEqual([])
    expect(sqsMock)
      .toHaveReceivedNthSpecificCommandWith(1, ListQueuesCommand, {
        QueueNamePrefix: 'sdqd_amzn_orders_1',
        MaxResults: 1000
      })
  })
})

describe('getQueueAttributes', () => {
  getSQSClient()
  const client = getSQSClient()

  test('smoke test', async () => {
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueAttributesCommand)
      .resolves({
        Attributes: {
          ApproximateNumberOfMessages: 10,
          ApproximateNumberOfMessagesDelayed: 1,
          ApproximateNumberOfMessagesNotVisible: 2
        }
      })
    await expect(
      getQueueAttributes([
        'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_amzn_orders_1_1021_failed',
        'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_amzn_orders_1_1022_failed'
      ])
    )
      .resolves.toEqual([
        {
          queue: 'sdqd_amzn_orders_1_1021_failed',
          result: {
            Attributes: {
              ApproximateNumberOfMessages: 10,
              ApproximateNumberOfMessagesDelayed: 1,
              ApproximateNumberOfMessagesNotVisible: 2
            }
          }
        },
        {
          queue: 'sdqd_amzn_orders_1_1022_failed',
          result: {
            Attributes: {
              ApproximateNumberOfMessages: 10,
              ApproximateNumberOfMessagesDelayed: 1,
              ApproximateNumberOfMessagesNotVisible: 2
            }
          }
        }
      ])
    expect(sqsMock)
    expect(sqsMock)
      .toHaveReceivedNthSpecificCommandWith(1, GetQueueAttributesCommand, {
        QueueUrl: 'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_amzn_orders_1_1021_failed',
        AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible', 'ApproximateNumberOfMessagesDelayed']
      })
    expect(sqsMock)
      .toHaveReceivedNthSpecificCommandWith(2, GetQueueAttributesCommand, {
        QueueUrl: 'https://sqs.us-east-1.amazonaws.com/foobar/sdqd_amzn_orders_1_1022_failed',
        AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible', 'ApproximateNumberOfMessagesDelayed']
      })
  })
})
