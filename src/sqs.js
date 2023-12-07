import { SQSClient } from '@aws-sdk/client-sqs'

/**
 * Utility function to return an instantiated, shared SQSClient.
 */
let client
export function getClient () {
  if (client) return client
  client = new SQSClient()
  return client
}

/**
 * Utility function to set the client explicitly, used in testing.
 */
export function setClient (explicitClient) {
  client = explicitClient
}
