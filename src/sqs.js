import { SQSClient } from '@aws-sdk/client-sqs'

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
