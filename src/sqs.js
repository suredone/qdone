/**
 * Functions that deal with SQS
 */

import { SQSClient, ListQueuesCommand, GetQueueAttributesCommand } from '@aws-sdk/client-sqs'
import { basename } from 'path'
import Debug from 'debug'
const debug = Debug('qdone:sqs')

/**
 * Utility function to return an instantiated, shared SQSClient.
 */
let client
export function getSQSClient () {
  if (client) return client
  client = new SQSClient()
  return client
}

/**
 * Utility function to set the client explicitly, used in testing.
 */
export function setSQSClient (explicitClient) {
  client = explicitClient
}

/**
 * Returns qrls for queues matching the given prefix and regex.
 */
export async function getMatchingQueues (prefix, regex) {
  const input = { QueueNamePrefix: prefix, MaxResults: 1000 }
  const client = getSQSClient()
  async function processQueues (nextToken) {
    if (nextToken) input.NextToken = nextToken
    const command = new ListQueuesCommand(input)
    // debug({ nextToken, input, command })
    const result = await client.send(command)
    // debug({ result })
    const { QueueUrls: qrls, NextToken: nextToken2 } = result
    // debug({ qrls, nextToken2 })
    return (qrls || []).filter(q => regex.test(q)).concat(nextToken2 ? await processQueues(nextToken2) : [])
  }
  return processQueues()
}

/**
 * Gets attributes on every queue in parallel.
 */
export async function getQueueAttributes (qrls) {
  const promises = []
  // debug({ qrls })
  for (const qrl of qrls) {
    const input = {
      QueueUrl: qrl,
      AttributeNames: [
        'ApproximateNumberOfMessages',
        'ApproximateNumberOfMessagesNotVisible',
        'ApproximateNumberOfMessagesDelayed'
      ]
    }
    const command = new GetQueueAttributesCommand(input)
    // debug({ input, command })
    promises.push((async () => {
      const queue = basename(qrl)
      const result = await client.send(command)
      // debug({ queue, result })
      return { queue, result }
    })())
  }
  return Promise.all(promises)
}

debug('loaded')
