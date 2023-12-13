import {
  CreateQueueCommand,
  GetQueueUrlCommand,
  GetQueueAttributesCommand,
  SendMessageCommand,
  SendMessageBatchCommand
} from '@aws-sdk/client-sqs'
import { getSQSClient, setSQSClient } from '../src/sqs.js'
import {
  getOrCreateFailQueue,
  getOrCreateQueue,
  getQueueAttributes,
  formatMessage,
  sendMessage,
  sendMessageBatch,
  addMessage,
  flushMessages,
  enqueue,
  enqueueBatch
} from '../src/enqueue.js'
import { qrlCacheSet, qrlCacheClear } from '../src/qrlCache.js'
import { getOptionsWithDefaults } from '../src/defaults.js'
import { mockClient } from 'aws-sdk-client-mock'
import 'aws-sdk-client-mock-jest'

getSQSClient()
const client = getSQSClient()

// Always clear qrl cache at the beginning of each test
beforeEach(qrlCacheClear)

describe('getOrCreateQueue', () => {
  test('cached qrl returns without calling api', async () => {
    const opt = getOptionsWithDefaults({ prefix: '' })
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const fqname = qname + opt.failSuffix
    const fqrl = qrl + opt.failSuffix
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    qrlCacheSet(qname, qrl)
    qrlCacheSet(fqname, fqrl)
    await expect(
      getOrCreateQueue(qname, opt)
    ).resolves.toBe(qrl)
    expect(sqsMock).toHaveReceivedCommandTimes(GetQueueAttributesCommand, 0)
    expect(sqsMock).toHaveReceivedCommandTimes(CreateQueueCommand, 0)
  })

  test('if queue exists, only makes GetQueueUrl api call', async () => {
    const opt = getOptionsWithDefaults({ prefix: '' })
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand, { QueueName: qname })
      .resolvesOnce({ QueueUrl: qrl })
      // .on(GetQueueUrlCommand, { QueueName: qname + opt.failSuffix })
      // .resolvesOnce({ QueueUrl: qrl + opt.failSuffix })
    await expect(
      getOrCreateQueue(qname, opt)
    ).resolves.toBe(qrl)
    expect(sqsMock).toHaveReceivedNthCommandWith(1, GetQueueUrlCommand, { QueueName: qname })
    // expect(sqsMock).toHaveReceivedNthCommandWith(2, GetQueueUrlCommand, { QueueName: qname + opt.failSuffix })
  })

  test('fifo variant of above works too', async () => {
    const opt = getOptionsWithDefaults({ prefix: '', fifo: true })
    const qname = 'testqueue.fifo'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand, { QueueName: qname })
      .resolvesOnce({ QueueUrl: qrl })
      // .on(GetQueueUrlCommand, { QueueName: qname + opt.failSuffix })
      // .resolvesOnce({ QueueUrl: qrl + opt.failSuffix })
    await expect(
      getOrCreateQueue(qname, opt)
    ).resolves.toBe(qrl)
    expect(sqsMock).toHaveReceivedNthCommandWith(1, GetQueueUrlCommand, { QueueName: qname })
    // expect(sqsMock).toHaveReceivedNthCommandWith(2, GetQueueUrlCommand, { QueueName: qname + opt.failSuffix })
  })

  test('if queue dne and fqueue exists, check fail queue arn and create queue', async () => {
    const opt = getOptionsWithDefaults({ prefix: '' })
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand, { QueueName: qname })
      .rejectsOnce({ name: 'QueueDoesNotExist' })
      .on(GetQueueUrlCommand, { QueueName: qname + opt.failSuffix })
      .resolvesOnce({ QueueUrl: qrl + opt.failSuffix })
      .on(GetQueueAttributesCommand, { QueueUrl: qrl + opt.failSuffix })
      .resolvesOnce({ Attributes: { QueueArn: 'foobar' } })
      .on(CreateQueueCommand, { QueueName: qname })
      .resolvesOnce({ QueueUrl: qrl })
    await expect(
      getOrCreateQueue(qname, opt)
    ).resolves.toBe(qrl)
    expect(sqsMock).toHaveReceivedNthCommandWith(1, GetQueueUrlCommand, { QueueName: qname })
    expect(sqsMock).toHaveReceivedNthCommandWith(2, GetQueueUrlCommand, { QueueName: qname + opt.failSuffix })
    expect(sqsMock).toHaveReceivedNthCommandWith(3, GetQueueAttributesCommand, { QueueUrl: qrl + opt.failSuffix })
    expect(sqsMock).toHaveReceivedNthCommandWith(4, CreateQueueCommand, {
      QueueName: qname,
      Attributes: {
        MessageRetentionPeriod: '1209600',
        RedrivePolicy: '{"deadLetterTargetArn":"foobar","maxReceiveCount":"1"}'
      }
    })
  })
})

describe('getOrCreateFailQueue', () => {
  test('cached qrl returns without calling api', async () => {
    const opt = getOptionsWithDefaults({ prefix: '' })
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const fqname = qname + opt.failSuffix
    const fqrl = qrl + opt.failSuffix
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    qrlCacheSet(fqname, fqrl)
    await expect(
      getOrCreateFailQueue(qname, opt)
    ).resolves.toBe(fqrl)
    await expect(
      getOrCreateFailQueue(fqname, opt)
    ).resolves.toBe(fqrl)
    expect(sqsMock).toHaveReceivedCommandTimes(GetQueueAttributesCommand, 0)
    expect(sqsMock).toHaveReceivedCommandTimes(CreateQueueCommand, 0)
  })

  test('if fqueue dne and dlq exists, get dlq arn and create fqueue', async () => {
    const opt = getOptionsWithDefaults({ prefix: '', dlq: true })
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const fqname = qname + opt.failSuffix
    const fqrl = qrl + opt.failSuffix
    const dqname = qname + opt.dlqSuffix
    const dqrl = qrl + opt.dlqSuffix
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand, { QueueName: fqname })
      .rejectsOnce({ name: 'QueueDoesNotExist' })
      .on(GetQueueUrlCommand, { QueueName: dqname })
      .resolvesOnce({ QueueUrl: dqrl })
      .on(GetQueueAttributesCommand, { QueueUrl: dqrl })
      .resolvesOnce({ Attributes: { QueueArn: 'foobar' } })
      .on(CreateQueueCommand, { QueueName: fqname })
      .resolvesOnce({ QueueUrl: fqrl })
    await expect(
      getOrCreateFailQueue(qname, opt)
    ).resolves.toBe(fqrl)
    expect(sqsMock).toHaveReceivedNthCommandWith(1, GetQueueUrlCommand, { QueueName: fqname })
    expect(sqsMock).toHaveReceivedNthCommandWith(2, GetQueueUrlCommand, { QueueName: dqname })
    expect(sqsMock).toHaveReceivedNthCommandWith(3, GetQueueAttributesCommand, { QueueUrl: dqrl })
    expect(sqsMock).toHaveReceivedNthCommandWith(4, CreateQueueCommand, {
      QueueName: fqname,
      Attributes: {
        MessageRetentionPeriod: '1209600',
        RedrivePolicy: '{"deadLetterTargetArn":"foobar","maxReceiveCount":"3"}'
      }
    })
  })

  test('fifo version of above', async () => {
    const opt = getOptionsWithDefaults({ prefix: '', dlq: true, fifo: true })
    const basename = 'testqueue'
    const qname = 'testqueue.fifo'
    const baseurl = `https://sqs.us-east-1.amazonaws.com/foobar/${basename}`
    const fqname = basename + opt.failSuffix + '.fifo'
    const fqrl = baseurl + opt.failSuffix + '.fifo'
    const dqname = basename + opt.dlqSuffix + '.fifo'
    const dqrl = baseurl + opt.dlqSuffix + '.fifo'
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand, { QueueName: fqname })
      .rejectsOnce({ name: 'QueueDoesNotExist' })
      .on(GetQueueUrlCommand, { QueueName: dqname })
      .resolvesOnce({ QueueUrl: dqrl })
      .on(GetQueueAttributesCommand, { QueueUrl: dqrl })
      .resolvesOnce({ Attributes: { QueueArn: 'foobar' } })
      .on(CreateQueueCommand, { QueueName: fqname })
      .resolvesOnce({ QueueUrl: fqrl })
    await expect(
      getOrCreateFailQueue(qname, opt)
    ).resolves.toBe(fqrl)
    expect(sqsMock).toHaveReceivedNthCommandWith(1, GetQueueUrlCommand, { QueueName: fqname })
    expect(sqsMock).toHaveReceivedNthCommandWith(2, GetQueueUrlCommand, { QueueName: dqname })
    expect(sqsMock).toHaveReceivedNthCommandWith(3, GetQueueAttributesCommand, { QueueUrl: dqrl })
    expect(sqsMock).toHaveReceivedNthCommandWith(4, CreateQueueCommand, {
      QueueName: fqname,
      Attributes: {
        FifoQueue: 'true',
        MessageRetentionPeriod: '1209600',
        RedrivePolicy: '{"deadLetterTargetArn":"foobar","maxReceiveCount":"3"}'
      }
    })
  })
})

describe('getQueueAttributes', () => {
  test('all attributes get queried', async () => {
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueAttributesCommand, { QueueUrl: qrl })
      .resolves({
        Attributes: {
          QueueArn: `arn:aws:sqs:us-east-1:foob ar:${qname}`,
          ApproximateNumberOfMessages: '0',
          ApproximateNumberOfMessagesNotVisible: '0',
          ApproximateNumberOfMessagesDelayed: '0',
          CreatedTimestamp: '1701880336',
          LastModifiedTimestamp: '1701880336',
          VisibilityTimeout: '30',
          MaximumMessageSize: '262144',
          MessageRetentionPeriod: '345600',
          DelaySeconds: '0',
          ReceiveMessageWaitTimeSeconds: '0',
          SqsManagedSseEnabled: 'true'
        }
      })
    expect(
      getQueueAttributes(qrl)
    ).resolves.toEqual({
      Attributes: {
        QueueArn: `arn:aws:sqs:us-east-1:foob ar:${qname}`,
        ApproximateNumberOfMessages: '0',
        ApproximateNumberOfMessagesNotVisible: '0',
        ApproximateNumberOfMessagesDelayed: '0',
        CreatedTimestamp: '1701880336',
        LastModifiedTimestamp: '1701880336',
        VisibilityTimeout: '30',
        MaximumMessageSize: '262144',
        MessageRetentionPeriod: '345600',
        DelaySeconds: '0',
        ReceiveMessageWaitTimeSeconds: '0',
        SqsManagedSseEnabled: 'true'
      }
    })
    expect(sqsMock)
      .toHaveReceivedNthSpecificCommandWith(1, GetQueueAttributesCommand, {
        QueueUrl: qrl,
        AttributeNames: ['All']
      })
  })
})

describe('formatMessage', () => {
  test('basic format is followed', async () => {
    const cmd = 'sd BulkStatusModel finalizeAll'
    expect(formatMessage(cmd)).toEqual({ MessageBody: cmd })
  })
  test('message with a specific id gets formatted', async () => {
    const cmd = 'sd BulkStatusModel finalizeAll'
    const id = '1234'
    expect(formatMessage(cmd, id)).toEqual({ MessageBody: cmd, Id: id })
  })
})

describe('sendMessage', () => {
  test('basic send works', async () => {
    const options = {}
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const cmd = 'sd BulkStatusModel finalizeAll'
    const sqsMock = mockClient(client)
    const messageId = '1e0632f4-b9e8-4f5c-a8e2-3529af1a56d6'
    const md5 = 'foobar'
    setSQSClient(sqsMock)
    sqsMock
      .on(SendMessageCommand, { QueueUrl: qrl })
      .resolves({ MD5OfMessageBody: md5, MessageId: messageId })
    await expect(
      sendMessage(qrl, cmd, options)
    ).resolves.toEqual({ MD5OfMessageBody: md5, MessageId: messageId })
    expect(sqsMock)
      .toHaveReceivedNthSpecificCommandWith(
        1,
        SendMessageCommand,
        Object.assign({ QueueUrl: qrl }, formatMessage(cmd))
      )
  })

  test('fifo send works', async () => {
    const groupId = 'foo'
    const deduplicationId = 'bar'
    const options = { fifo: true, 'group-id': groupId, 'deduplication-id': deduplicationId }
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const cmd = 'sd BulkStatusModel finalizeAll'
    const sqsMock = mockClient(client)
    const messageId = '1e0632f4-b9e8-4f5c-a8e2-3529af1a56d6'
    const md5 = 'foobar'
    setSQSClient(sqsMock)
    sqsMock
      .on(SendMessageCommand, { QueueUrl: qrl })
      .resolves({ MD5OfMessageBody: md5, MessageId: messageId })
    await expect(
      sendMessage(qrl, cmd, options)
    ).resolves.toEqual({ MD5OfMessageBody: md5, MessageId: messageId })
    expect(sqsMock)
      .toHaveReceivedNthCommandWith(
        1,
        SendMessageCommand,
        Object.assign({ QueueUrl: qrl, MessageGroupId: groupId }, formatMessage(cmd))
      )
  })

  test('delay option works', async () => {
    const groupId = 'foo'
    const deduplicationId = 'bar'
    const options = {
      delay: 15,
      fifo: true,
      'group-id': groupId,
      'deduplication-id': deduplicationId
    }
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const cmd = 'sd BulkStatusModel finalizeAll'
    const sqsMock = mockClient(client)
    const messageId = '1e0632f4-b9e8-4f5c-a8e2-3529af1a56d6'
    const md5 = 'foobar'
    setSQSClient(sqsMock)
    sqsMock
      .on(SendMessageCommand, { QueueUrl: qrl })
      .resolves({ MD5OfMessageBody: md5, MessageId: messageId })
    await expect(
      sendMessage(qrl, cmd, options)
    ).resolves.toEqual({ MD5OfMessageBody: md5, MessageId: messageId })
    expect(sqsMock)
      .toHaveReceivedNthCommandWith(
        1,
        SendMessageCommand,
        Object.assign({
          QueueUrl: qrl,
          MessageGroupId: groupId,
          DelaySeconds: options.delay
        }, formatMessage(cmd))
      )
  })
})

describe('sendMessageBatch', () => {
  test('basic batch works', async () => {
    const options = {}
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const cmd = 'sd BulkStatusModel finalizeAll'
    const sqsMock = mockClient(client)
    const messageId = '1e0632f4-b9e8-4f5c-a8e2-3529af1a56d6'
    const md5 = 'foobar'
    const messages = [
      formatMessage(cmd),
      formatMessage(cmd)
    ]
    setSQSClient(sqsMock)
    sqsMock
      .on(SendMessageBatchCommand, { QueueUrl: qrl })
      .resolves({ MD5OfMessageBody: md5, MessageId: messageId })
    await expect(
      sendMessageBatch(qrl, messages, options)
    ).resolves.toEqual({ MD5OfMessageBody: md5, MessageId: messageId })
    expect(sqsMock)
      .toHaveReceivedNthSpecificCommandWith(
        1,
        SendMessageBatchCommand,
        Object.assign({ QueueUrl: qrl, Entries: messages })
      )
  })

  test('batch with delay works', async () => {
    const options = { delay: 15 }
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const cmd = 'sd BulkStatusModel finalizeAll'
    const sqsMock = mockClient(client)
    const messageId = '1e0632f4-b9e8-4f5c-a8e2-3529af1a56d6'
    const md5 = 'foobar'
    const messages = [
      formatMessage(cmd),
      formatMessage(cmd)
    ]
    messages[0].DelaySeconds = options.delay
    messages[1].DelaySeconds = options.delay
    setSQSClient(sqsMock)
    sqsMock
      .on(SendMessageBatchCommand, { QueueUrl: qrl })
      .resolves({ MD5OfMessageBody: md5, MessageId: messageId })
    await expect(
      sendMessageBatch(qrl, messages, options)
    ).resolves.toEqual({ MD5OfMessageBody: md5, MessageId: messageId })
    expect(sqsMock)
      .toHaveReceivedNthSpecificCommandWith(
        1,
        SendMessageBatchCommand,
        Object.assign({ QueueUrl: qrl, Entries: messages })
      )
  })

  test('fifo batch works', async () => {
    const messageId = '1e0632f4-b9e8-4f5c-a8e2-3529af1a56d6'
    const groupId = 'buzz'
    const options = {
      prefix: '',
      fifo: true,
      'group-id': groupId,
      uuidFunction: () => messageId
    }
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const cmd = 'sd BulkStatusModel finalizeAll'
    const sqsMock = mockClient(client)
    const md5 = 'foobar'
    const messages = [
      Object.assign({ MessageDeduplicationId: messageId, MessageGroupId: groupId }, formatMessage(cmd)),
      Object.assign({ MessageDeduplicationId: messageId, MessageGroupId: groupId }, formatMessage(cmd))
    ]
    setSQSClient(sqsMock)
    sqsMock
      .on(SendMessageBatchCommand, { QueueUrl: qrl })
      .resolves({
        Succeeded: [
          { MD5OfMessageBody: md5, MessageId: messageId, MessageGroupId: groupId },
          { MD5OfMessageBody: md5, MessageId: messageId, MessageGroupId: groupId }
        ]
      })
    await expect(
      sendMessageBatch(qrl, messages, options)
    ).resolves.toEqual({
      Succeeded: [
        { MD5OfMessageBody: md5, MessageId: messageId, MessageGroupId: groupId },
        { MD5OfMessageBody: md5, MessageId: messageId, MessageGroupId: groupId }
      ]
    })
    expect(sqsMock)
      .toHaveReceivedNthSpecificCommandWith(
        1,
        SendMessageBatchCommand,
        Object.assign({ QueueUrl: qrl, Entries: messages })
      )
  })

  test('fifo batch with group-id-per-message works', async () => {
    const messageId = '1e0632f4-b9e8-4f5c-a8e2-3529af1a56d6'
    const groupId = 'buzz'
    const options = {
      prefix: '',
      'group-id-per-message': true,
      uuidFunction: () => messageId
    }
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const cmd = 'sd BulkStatusModel finalizeAll'
    const sqsMock = mockClient(client)
    const md5 = 'foobar'
    const messages = [
      Object.assign({ MessageDeduplicationId: messageId, MessageGroupId: groupId }, formatMessage(cmd)),
      Object.assign({ MessageDeduplicationId: messageId, MessageGroupId: groupId }, formatMessage(cmd))
    ]
    setSQSClient(sqsMock)
    sqsMock
      .on(SendMessageBatchCommand, { QueueUrl: qrl })
      .resolves({
        Succeeded: [
          { MD5OfMessageBody: md5, MessageId: messageId, MessageGroupId: groupId },
          { MD5OfMessageBody: md5, MessageId: messageId, MessageGroupId: groupId }
        ]
      })
    await expect(
      sendMessageBatch(qrl, messages, options)
    ).resolves.toEqual({
      Succeeded: [
        { MD5OfMessageBody: md5, MessageId: messageId, MessageGroupId: groupId },
        { MD5OfMessageBody: md5, MessageId: messageId, MessageGroupId: groupId }
      ]
    })
    expect(sqsMock)
      .toHaveReceivedNthSpecificCommandWith(
        1,
        SendMessageBatchCommand,
        Object.assign({ QueueUrl: qrl, Entries: messages })
      )
  })
})

describe('addMessage / flushMessages', () => {
  test('basic add/flush cycle works', async () => {
    const options = {}
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const cmd = 'sd BulkStatusModel finalizeAll'
    const sqsMock = mockClient(client)
    const messageId = '1e0632f4-b9e8-4f5c-a8e2-3529af1a56d6'
    const md5 = 'foobar'

    // First 9 should not flush
    expect(addMessage(qrl, cmd, options)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, options)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, options)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, options)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, options)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, options)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, options)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, options)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, options)).resolves.toBe(0)

    // Now we should see a flush
    setSQSClient(sqsMock)
    sqsMock
      .on(SendMessageBatchCommand, { QueueUrl: qrl })
      .resolvesOnce({
        Successful: [
          { MD5OfMessageBody: md5, MessageId: messageId },
          { MD5OfMessageBody: md5, MessageId: messageId },
          { MD5OfMessageBody: md5, MessageId: messageId },
          { MD5OfMessageBody: md5, MessageId: messageId },
          { MD5OfMessageBody: md5, MessageId: messageId },
          { MD5OfMessageBody: md5, MessageId: messageId },
          { MD5OfMessageBody: md5, MessageId: messageId },
          { MD5OfMessageBody: md5, MessageId: messageId },
          { MD5OfMessageBody: md5, MessageId: messageId },
          { MD5OfMessageBody: md5, MessageId: messageId }
        ]
      })
      .resolvesOnce({
        Successful: [
          { MD5OfMessageBody: md5, MessageId: messageId },
          { MD5OfMessageBody: md5, MessageId: messageId },
          { MD5OfMessageBody: md5, MessageId: messageId }
        ]
      })

    // And the next one should flush all 10
    expect(addMessage(qrl, cmd, options)).resolves.toBe(10)

    // And add three more
    expect(addMessage(qrl, cmd, options)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, options)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, options)).resolves.toBe(0)
    // should flush those three
    await expect(flushMessages(qrl, options)).resolves.toBe(3)
    expect(sqsMock)
      .toHaveReceivedNthSpecificCommandWith(
        1,
        SendMessageBatchCommand,
        Object.assign({
          QueueUrl: qrl,
          Entries: [
            formatMessage(cmd, 0),
            formatMessage(cmd, 1),
            formatMessage(cmd, 2),
            formatMessage(cmd, 3),
            formatMessage(cmd, 4),
            formatMessage(cmd, 5),
            formatMessage(cmd, 6),
            formatMessage(cmd, 7),
            formatMessage(cmd, 8),
            formatMessage(cmd, 9)
          ]
        })
      )
    expect(sqsMock)
      .toHaveReceivedNthSpecificCommandWith(
        2,
        SendMessageBatchCommand,
        Object.assign({
          QueueUrl: qrl,
          Entries: [
            formatMessage(cmd, 10),
            formatMessage(cmd, 11),
            formatMessage(cmd, 12)
          ]
        })
      )
  })

  test('failed messages fail the whole batch', async () => {
    const options = {}
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const cmd = 'sd BulkStatusModel finalizeAll'
    const sqsMock = mockClient(client)
    const messageId = '1e0632f4-b9e8-4f5c-a8e2-3529af1a56d6'
    const md5 = 'foobar'

    // First 9 should not flush
    expect(addMessage(qrl, cmd, options)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, options)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, options)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, options)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, options)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, options)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, options)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, options)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, options)).resolves.toBe(0)

    // Now we should see a flush
    setSQSClient(sqsMock)
    sqsMock
      .on(SendMessageBatchCommand, { QueueUrl: qrl })
      .resolvesOnce({
        Successful: [
          { MD5OfMessageBody: md5, MessageId: messageId },
          { MD5OfMessageBody: md5, MessageId: messageId },
          { MD5OfMessageBody: md5, MessageId: messageId },
          { MD5OfMessageBody: md5, MessageId: messageId },
          { MD5OfMessageBody: md5, MessageId: messageId },
          { MD5OfMessageBody: md5, MessageId: messageId },
          { MD5OfMessageBody: md5, MessageId: messageId },
          { MD5OfMessageBody: md5, MessageId: messageId },
          { MD5OfMessageBody: md5, MessageId: messageId },
          { MD5OfMessageBody: md5, MessageId: messageId }
        ]
      })
      .resolvesOnce({
        Successful: [
          { MD5OfMessageBody: md5, MessageId: messageId },
          { MD5OfMessageBody: md5, MessageId: messageId }
        ],
        Failed: [
          { SenderFault: true, Id: '25', Code: 'XYZ', Message: 'You messed up.' }
        ]
      })

    // And the next one should flush all 10
    expect(addMessage(qrl, cmd, options)).resolves.toBe(10)

    // And add three more
    expect(addMessage(qrl, cmd, options)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, options)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, options)).resolves.toBe(0)
    // should flush those three
    await expect(flushMessages(qrl, options))
      .rejects.toThrow('One or more message failures')
    expect(sqsMock)
      .toHaveReceivedNthSpecificCommandWith(
        1,
        SendMessageBatchCommand,
        Object.assign({
          QueueUrl: qrl,
          Entries: [
            formatMessage(cmd, 0 + 13),
            formatMessage(cmd, 1 + 13),
            formatMessage(cmd, 2 + 13),
            formatMessage(cmd, 3 + 13),
            formatMessage(cmd, 4 + 13),
            formatMessage(cmd, 5 + 13),
            formatMessage(cmd, 6 + 13),
            formatMessage(cmd, 7 + 13),
            formatMessage(cmd, 8 + 13),
            formatMessage(cmd, 9 + 13)
          ]
        })
      )
    expect(sqsMock)
      .toHaveReceivedNthSpecificCommandWith(
        2,
        SendMessageBatchCommand,
        Object.assign({
          QueueUrl: qrl,
          Entries: [
            formatMessage(cmd, 10 + 13),
            formatMessage(cmd, 11 + 13),
            formatMessage(cmd, 12 + 13)
          ]
        })
      )
  })
})

describe('enqueue', () => {
  test('basic enqueue works', async () => {
    const options = { prefix: '' }
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const cmd = 'sd BulkStatusModel finalizeAll'
    const sqsMock = mockClient(client)
    const messageId = '1e0632f4-b9e8-4f5c-a8e2-3529af1a56d6'
    const md5 = 'foobar'
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand, { QueueName: qname })
      .resolves({ QueueUrl: qrl })
      .on(SendMessageCommand, { QueueUrl: qrl })
      .resolves({ MD5OfMessageBody: md5, MessageId: messageId })
    await expect(
      enqueue(qname, cmd, options)
    ).resolves.toEqual({ MD5OfMessageBody: md5, MessageId: messageId })
    expect(sqsMock)
      .toHaveReceivedNthCommandWith(1, GetQueueUrlCommand, { QueueName: qname })
    expect(sqsMock)
      .toHaveReceivedNthCommandWith(
        2,
        SendMessageCommand,
        Object.assign({ QueueUrl: qrl }, formatMessage(cmd))
      )
  })
})

describe('enqueueBatch', () => {
  test('basic enqueueBatch works', async () => {
    const messageId = '1e0632f4-b9e8-4f5c-a8e2-3529af1a56d6'
    const options = {
      prefix: '',
      'fail-suffix': '_failed',
      uuidFunction: () => messageId
    }
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const cmd = 'sd BulkStatusModel finalizeAll'
    const sqsMock = mockClient(client)
    const md5 = 'foobar'
    const pairs = [
      { queue: qname, command: cmd },
      { queue: qname, command: cmd }
    ]
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand, { QueueName: qname })
      .resolves({ QueueUrl: qrl })
      .on(SendMessageBatchCommand, { QueueUrl: qrl })
      .resolves({
        Successful: [
          { MD5OfMessageBody: md5, MessageId: messageId },
          { MD5OfMessageBody: md5, MessageId: messageId }
        ]
      })
    await expect(
      enqueueBatch(pairs, options)
    ).resolves.toBe(2)
    expect(sqsMock)
      .toHaveReceivedNthCommandWith(1, GetQueueUrlCommand, { QueueName: qname })
    expect(sqsMock)
      .toHaveReceivedNthCommandWith(2, SendMessageBatchCommand, {
        QueueUrl: qrl,
        Entries: [
          { MessageBody: cmd, Id: '26' },
          { MessageBody: cmd, Id: '27' }
        ]
      })
  })
})
