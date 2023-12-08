import { CloudWatchClient } from '@aws-sdk/client-cloudwatch'

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
