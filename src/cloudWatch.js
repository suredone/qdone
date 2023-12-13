/**
 * Functions that deal with CloudWatch
 */

import { CloudWatchClient, PutMetricDataCommand } from '@aws-sdk/client-cloudwatch'
import Debug from 'debug'
const debug = Debug('qdone:cloudWatch')

/**
 * Utility function to return an instantiated, shared CloudWatchClient.
 */
let client
export function getCloudWatchClient () {
  if (client) return client
  client = new CloudWatchClient()
  return client
}

/**
 * Utility function to set the client explicitly, used in testing.
 */
export function setCloudWatchClient (explicitClient) {
  client = explicitClient
}

/**
 * Takes data in the form returned by getAggregageData() and pushes it to
 * CloudWatch metrics under the given queueName.
 *
 * @param queueName {String} - The name of the wildcard queue these metrics are for.
 * @param total {Object} - returned object from getAggregateData()
 */
export async function putAggregateData (total, timestamp) {
  const client = getCloudWatchClient()
  const now = timestamp || new Date()
  const input = {
    Namespace: 'qmonitor',
    MetricData: [
      {
        MetricName: 'totalQueues',
        Dimensions: [{
          Name: 'queueName',
          Value: total.queueName
        }],
        Timestamp: now,
        Value: total.totalQueues,
        Unit: 'Count'
      },
      {
        MetricName: 'contributingQueueCount',
        Dimensions: [{
          Name: 'queueName',
          Value: total.queueName
        }],
        Timestamp: now,
        Value: total.contributingQueueNames.length,
        Unit: 'Count'
      },
      {
        MetricName: 'ApproximateNumberOfMessages',
        Dimensions: [{
          Name: 'queueName',
          Value: total.queueName
        }],
        Timestamp: now,
        Value: total.ApproximateNumberOfMessages || 0,
        Unit: 'Count'
      },
      {
        MetricName: 'ApproximateNumberOfMessagesDelayed',
        Dimensions: [{
          Name: 'queueName',
          Value: total.queueName
        }],
        Timestamp: now,
        Value: total.ApproximateNumberOfMessagesDelayed || 0,
        Unit: 'Count'
      },
      {
        MetricName: 'ApproximateNumberOfMessagesNotVisible',
        Dimensions: [{
          Name: 'queueName',
          Value: total.queueName
        }],
        Timestamp: now,
        Value: total.ApproximateNumberOfMessagesNotVisible || 0,
        Unit: 'Count'
      }
    ]
  }
  const command = new PutMetricDataCommand(input)
  // debug({ input, command })
  const response = await client.send(command)
  debug({ response })
}

debug('loaded')
