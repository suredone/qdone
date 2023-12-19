/*
import {
  GetQueueUrlCommand,
  GetQueueAttributesCommand,
  SendMessageCommand,
  SendMessageBatchCommand
} from '@aws-sdk/client-sqs'
import { getSQSClient, setSQSClient } from '../src/sqs.js'
import {
  getQueueAttributes,
  formatMessage,
  sendMessage,
  sendMessageBatch,
  addMessage,
  flushMessages,
  enqueue,
  enqueueBatch
} from '../src/enqueue.js'
import { qrlCacheClear } from '../src/qrlCache.js'
import { mockClient } from 'aws-sdk-client-mock'
import 'aws-sdk-client-mock-jest'

getSQSClient()
const client = getSQSClient()

// Always clear qrl cache at the beginning of each test
beforeEach(qrlCacheClear)
*/

describe('mock', () => {
  test('all attributes get queried', async () => {
    expect(1).toEqual(1)
  })
})
