/**
 * Functions that deal with SQS
 */

import { SQSClient, ListQueuesCommand, GetQueueAttributesCommand, QueueDoesNotExist } from '@aws-sdk/client-sqs'
import { NodeHttpHandler } from '@smithy/node-http-handler'
import { basename } from 'path'
import Debug from 'debug'
const debug = Debug('qdone:sqs')

// Long-poll receives use WaitTimeSeconds up to the SQS maximum of 20s, so give
// the socket comfortably more than that before treating it as hung. Without an
// explicit requestTimeout a silently-dropped TCP connection leaves a receive
// pending forever — the consumer then leaks that receive's concurrency
// accounting and eventually goes deaf. connectionTimeout guards the TCP handshake.
const maxLongPollSeconds = 20
const requestTimeout = (maxLongPollSeconds + 20) * 1000
const connectionTimeout = 5000

/**
 * Utility function to return an instantiated, shared SQSClient.
 */
let client
export function getSQSClient () {
  if (client) return client
  client = new SQSClient({
    requestHandler: new NodeHttpHandler({ connectionTimeout, requestTimeout })
  })
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
      try {
        const result = await client.send(command)
        // debug({ queue, result })
        return { queue, result }
      } catch (e) {
        if (e instanceof QueueDoesNotExist) {
          // For queues that have been deleted in the meantime for whatever
          // reason, just show as having no messages instead of failing the
          // whole batch
          return {
            queue,
            Attributes: {
              ApproximateNumberOfMessages: '0',
              ApproximateNumberOfMessagesNotVisible: '0',
              ApproximateNumberOfMessagesDelayed: '0'
            }
          }
        }
        throw e
      }
    })())
  }
  return Promise.all(promises)
}

debug('loaded')
