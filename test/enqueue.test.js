import { jest } from '@jest/globals'
import {
  CreateQueueCommand,
  GetQueueUrlCommand,
  GetQueueAttributesCommand,
  SendMessageCommand,
  SendMessageBatchCommand,
  QueueDoesNotExist,
  RequestThrottled
} from '@aws-sdk/client-sqs'
import { mockClient } from 'aws-sdk-client-mock'
import 'aws-sdk-client-mock-jest'

import {
  getOrCreateFailQueue,
  getOrCreateQueue,
  getOrCreateDLQ,
  getQueueAttributes,
  formatMessage,
  sendMessage,
  sendMessageBatch,
  addMessage,
  flushMessages,
  enqueue,
  enqueueBatch
} from '../src/enqueue.js'
import { getSQSClient, setSQSClient } from '../src/sqs.js'
import { qrlCacheSet, qrlCacheClear } from '../src/qrlCache.js'
import { getOptionsWithDefaults } from '../src/defaults.js'
import { loadBatchFiles } from '../src/cli.js'

getSQSClient()
const client = getSQSClient()
jest.useFakeTimers()

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
    const opt = getOptionsWithDefaults({ prefix: '', fifo: true, verbose: true })
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

  test('unexpected errors are re-thrown', async () => {
    const opt = getOptionsWithDefaults({ prefix: '' })
    const qname = 'testqueue'
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand, { QueueName: qname })
      .rejectsOnce(new Error('something unexpected'))
    await expect(
      getOrCreateQueue(qname, opt)
    ).rejects.toThrow('something unexpected')
    expect(sqsMock).toHaveReceivedNthCommandWith(1, GetQueueUrlCommand, { QueueName: qname })
  })

  test('if queue dne and fqueue exists, check fail queue arn and create queue', async () => {
    const opt = getOptionsWithDefaults({ prefix: '' })
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand, { QueueName: qname })
      .rejectsOnce(new QueueDoesNotExist())
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

  test('fifo variant of above', async () => {
    console.error('fifo variant')
    const opt = getOptionsWithDefaults({ prefix: '', dlq: true, fifo: true, verbose: true })
    const basename = 'testqueue'
    const qname = basename + '.fifo'
    const baseurl = `https://sqs.us-east-1.amazonaws.com/foobar/${basename}`
    const qrl = baseurl + '.fifo'
    const fqname = basename + opt.failSuffix + '.fifo'
    const fqrl = baseurl + opt.failSuffix + '.fifo'
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand, { QueueName: qname })
      .rejectsOnce(new QueueDoesNotExist())
      .on(GetQueueUrlCommand, { QueueName: fqname })
      .resolvesOnce({ QueueUrl: fqrl })
      .on(GetQueueAttributesCommand, { QueueUrl: fqrl })
      .resolvesOnce({ Attributes: { QueueArn: 'foobar' } })
      .on(CreateQueueCommand, { QueueName: qname })
      .resolvesOnce({ QueueUrl: qrl })
    await expect(
      getOrCreateQueue(basename, opt)
    ).resolves.toBe(qrl)
    expect(sqsMock).toHaveReceivedNthCommandWith(1, GetQueueUrlCommand, { QueueName: qname })
    expect(sqsMock).toHaveReceivedNthCommandWith(2, GetQueueUrlCommand, { QueueName: fqname })
    expect(sqsMock).toHaveReceivedNthCommandWith(3, GetQueueAttributesCommand, { QueueUrl: fqrl })
    expect(sqsMock).toHaveReceivedNthCommandWith(4, CreateQueueCommand, {
      QueueName: qname,
      Attributes: {
        FifoQueue: 'true',
        MessageRetentionPeriod: '1209600',
        RedrivePolicy: '{"deadLetterTargetArn":"foobar","maxReceiveCount":"1"}'
      }
    })
  })

  test('tag variant of above', async () => {
    console.error('fifo variant')
    const tags = { Role: 'app', Environment: 'production' }
    const opt = getOptionsWithDefaults({ prefix: '', dlq: true, fifo: true, verbose: true, tags })
    const basename = 'testqueue'
    const qname = basename + '.fifo'
    const baseurl = `https://sqs.us-east-1.amazonaws.com/foobar/${basename}`
    const qrl = baseurl + '.fifo'
    const fqname = basename + opt.failSuffix + '.fifo'
    const fqrl = baseurl + opt.failSuffix + '.fifo'
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand, { QueueName: qname })
      .rejectsOnce(new QueueDoesNotExist())
      .on(GetQueueUrlCommand, { QueueName: fqname })
      .resolvesOnce({ QueueUrl: fqrl })
      .on(GetQueueAttributesCommand, { QueueUrl: fqrl })
      .resolvesOnce({ Attributes: { QueueArn: 'foobar' } })
      .on(CreateQueueCommand, { QueueName: qname })
      .resolvesOnce({ QueueUrl: qrl })
    await expect(
      getOrCreateQueue(basename, opt)
    ).resolves.toBe(qrl)
    expect(sqsMock).toHaveReceivedNthCommandWith(1, GetQueueUrlCommand, { QueueName: qname })
    expect(sqsMock).toHaveReceivedNthCommandWith(2, GetQueueUrlCommand, { QueueName: fqname })
    expect(sqsMock).toHaveReceivedNthCommandWith(3, GetQueueAttributesCommand, { QueueUrl: fqrl })
    expect(sqsMock).toHaveReceivedNthCommandWith(4, CreateQueueCommand, {
      QueueName: qname,
      Attributes: {
        FifoQueue: 'true',
        MessageRetentionPeriod: '1209600',
        RedrivePolicy: '{"deadLetterTargetArn":"foobar","maxReceiveCount":"1"}'
      },
      tags
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
      .rejectsOnce(new QueueDoesNotExist())
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

  test('unexpected errors are re-thrown', async () => {
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
      .rejectsOnce(new Error('something unexpected'))
      .on(GetQueueUrlCommand, { QueueName: dqname })
      .resolvesOnce({ QueueUrl: dqrl })
      .on(GetQueueAttributesCommand, { QueueUrl: dqrl })
      .resolvesOnce({ Attributes: { QueueArn: 'foobar' } })
      .on(CreateQueueCommand, { QueueName: fqname })
      .resolvesOnce({ QueueUrl: fqrl })
    await expect(
      getOrCreateFailQueue(qname, opt)
    ).rejects.toThrow('something unexpected')
    expect(sqsMock).toHaveReceivedNthCommandWith(1, GetQueueUrlCommand, { QueueName: fqname })
  })

  test('fifo version of above', async () => {
    const opt = getOptionsWithDefaults({ prefix: '', dlq: true, fifo: true, verbose: true })
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
      .rejectsOnce(new QueueDoesNotExist())
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

describe('getOrCreateDLQ', () => {
  test('cached qrl returns without calling api', async () => {
    const opt = getOptionsWithDefaults({ prefix: '', dlq: true })
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const dqname = qname + opt.dlqSuffix
    const dqrl = qrl + opt.dlqSuffix
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    qrlCacheSet(dqname, dqrl)
    await expect(
      getOrCreateDLQ(qname, opt)
    ).resolves.toBe(dqrl)
    expect(sqsMock).toHaveReceivedCommandTimes(GetQueueAttributesCommand, 0)
    expect(sqsMock).toHaveReceivedCommandTimes(CreateQueueCommand, 0)
  })

  test('if dlq dne, create it', async () => {
    const opt = getOptionsWithDefaults({ prefix: '', dlq: true })
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const dqname = qname + opt.dlqSuffix
    const dqrl = qrl + opt.dlqSuffix
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand, { QueueName: dqname })
      .rejectsOnce(new QueueDoesNotExist())
      .on(CreateQueueCommand, { QueueName: dqname })
      .resolvesOnce({ QueueUrl: dqrl })
    await expect(
      getOrCreateDLQ(qname, opt)
    ).resolves.toBe(dqrl)
    expect(sqsMock).toHaveReceivedNthCommandWith(1, GetQueueUrlCommand, { QueueName: dqname })
    expect(sqsMock).toHaveReceivedNthCommandWith(2, CreateQueueCommand, {
      QueueName: dqname,
      Attributes: { MessageRetentionPeriod: '1209600' }
    })
  })

  test('fifo version of above', async () => {
    const opt = getOptionsWithDefaults({ prefix: '', dlq: true, fifo: true, verbose: true })
    const basename = 'testqueue'
    const qname = 'testqueue.fifo'
    const baseurl = `https://sqs.us-east-1.amazonaws.com/foobar/${basename}`
    const dqname = basename + opt.dlqSuffix + '.fifo'
    const dqrl = baseurl + opt.dlqSuffix + '.fifo'
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand, { QueueName: dqname })
      .rejectsOnce(new QueueDoesNotExist())
      .on(CreateQueueCommand, { QueueName: dqname })
      .resolvesOnce({ QueueUrl: dqrl })
    await expect(
      getOrCreateDLQ(qname, opt)
    ).resolves.toBe(dqrl)
    expect(sqsMock).toHaveReceivedNthCommandWith(1, GetQueueUrlCommand, { QueueName: dqname })
    expect(sqsMock).toHaveReceivedNthCommandWith(2, CreateQueueCommand, {
      QueueName: dqname,
      Attributes: { FifoQueue: 'true', MessageRetentionPeriod: '1209600' }
    })
  })

  test('unexpected errors are re-thrown', async () => {
    const opt = getOptionsWithDefaults({ prefix: '', dlq: true })
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const dqname = qname + opt.dlqSuffix
    const dqrl = qrl + opt.dlqSuffix
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand, { QueueName: dqname })
      .rejectsOnce(new Error('something unexpected'))
      .on(CreateQueueCommand, { QueueName: dqname })
      .resolvesOnce({ QueueUrl: dqrl })
    await expect(
      getOrCreateDLQ(qname, opt)
    ).rejects.toThrow('something unexpected')
    expect(sqsMock).toHaveReceivedNthCommandWith(1, GetQueueUrlCommand, { QueueName: dqname })
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
    const opt = getOptionsWithDefaults(options)
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
      sendMessage(qrl, cmd, opt)
    ).resolves.toEqual({ MD5OfMessageBody: md5, MessageId: messageId })
    expect(sqsMock).toHaveReceivedNthCommandWith(
      1, SendMessageCommand,
      Object.assign({}, formatMessage(cmd), { QueueUrl: qrl, MessageGroupId: opt.groupId })
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
    const opt = getOptionsWithDefaults(options)
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
      sendMessage(qrl, cmd, opt)
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

  test('retryable exceptions cause retry', async () => {
    const groupId = 'foo'
    const deduplicationId = 'bar'
    const options = {
      delay: 15,
      fifo: true,
      'group-id': groupId,
      'deduplication-id': deduplicationId
    }
    const opt = getOptionsWithDefaults(options)
    const qname = 'testqueue'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const cmd = 'sd BulkStatusModel finalizeAll'
    const sqsMock = mockClient(client)
    const messageId = '1e0632f4-b9e8-4f5c-a8e2-3529af1a56d6'
    const md5 = 'foobar'
    setSQSClient(sqsMock)
    sqsMock
      .on(SendMessageCommand, { QueueUrl: qrl })
      .rejectsOnce(new RequestThrottled())
      // .rejectsOnce(new KmsThrottled())
      // .rejectsOnce(new QueueDoesNotExist())
      .resolvesOnce({ MD5OfMessageBody: md5, MessageId: messageId })
    const promise = sendMessage(qrl, cmd, opt)

    await Promise.resolve() // shouldRetry()
    await Promise.resolve() // await this.delay(attemptNumber)
    jest.runAllTimers() // delay() -> setTimeout()

    await Promise.resolve() // await action
    jest.runAllTimers() // not sure why here

    await expect(promise).resolves.toEqual({ MD5OfMessageBody: md5, MessageId: messageId })
    expect(sqsMock)
      .toHaveReceivedNthCommandWith(
        2,
        SendMessageCommand,
        Object.assign({
          QueueUrl: qrl,
          MessageGroupId: groupId,
          DelaySeconds: options.delay,
          MessageDeduplicationId: deduplicationId
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
    const sendBuffer = {}

    // First 9 should not flush
    expect(addMessage(qrl, cmd, 0, options, sendBuffer)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, 1, options, sendBuffer)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, 2, options, sendBuffer)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, 3, options, sendBuffer)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, 4, options, sendBuffer)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, 5, options, sendBuffer)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, 6, options, sendBuffer)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, 7, options, sendBuffer)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, 8, options, sendBuffer)).resolves.toBe(0)

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
    expect(addMessage(qrl, cmd, 9, options, sendBuffer)).resolves.toBe(10)

    // And add three more
    expect(addMessage(qrl, cmd, 10, options, sendBuffer)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, 11, options, sendBuffer)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, 12, options, sendBuffer)).resolves.toBe(0)
    // should flush those three
    await expect(flushMessages(qrl, options, sendBuffer)).resolves.toBe(3)
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
    const sendBuffer = {}

    // First 9 should not flush
    expect(addMessage(qrl, cmd, 0, options, sendBuffer)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, 1, options, sendBuffer)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, 2, options, sendBuffer)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, 3, options, sendBuffer)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, 4, options, sendBuffer)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, 5, options, sendBuffer)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, 6, options, sendBuffer)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, 7, options, sendBuffer)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, 8, options, sendBuffer)).resolves.toBe(0)

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
    expect(addMessage(qrl, cmd, 9, options, sendBuffer)).resolves.toBe(10)

    // And add three more
    expect(addMessage(qrl, cmd, 10, options, sendBuffer)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, 11, options, sendBuffer)).resolves.toBe(0)
    expect(addMessage(qrl, cmd, 12, options, sendBuffer)).resolves.toBe(0)
    // should flush those three
    await expect(flushMessages(qrl, options, sendBuffer))
      .rejects.toThrow('One or more message failures')
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

  test('enqueue with creation', async () => {
    const opt = getOptionsWithDefaults()
    const messageId = '1e0632f4-b9e8-4f5c-a8e2-3529af1a56d6'
    const md5 = '51b0a325...39163aa0'
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand, { QueueName: 'qdone_testQueue_failed' })
      .rejectsOnce(new QueueDoesNotExist())
      .resolvesOnce({ QueueUrl: 'https://q.amazonaws.com/123456789101/qdone_testQueue_failed' })
      .on(GetQueueUrlCommand, { QueueName: 'qdone_testQueue' })
      .rejectsOnce(new QueueDoesNotExist())
      .resolvesOnce({ QueueUrl: 'https://q.amazonaws.com/123456789101/qdone_testQueue' })
      .on(CreateQueueCommand)
      .resolvesOnce({ QueueUrl: 'https://q.amazonaws.com/123456789101/qdone_testQueue_failed' })
      .resolvesOnce({ QueueUrl: 'https://q.amazonaws.com/123456789101/qdone_testQueue' })
      .on(GetQueueAttributesCommand, { QueueUrl: 'https://q.amazonaws.com/123456789101/qdone_testQueue_failed' })
      .resolvesOnce({
        Attributes: {
          CreatedTimestamp: '1442426968',
          DelaySeconds: '0',
          LastModifiedTimestamp: '1442426968',
          MaximumMessageSize: '262144',
          MessageRetentionPeriod: '345600',
          QueueArn: 'arn:aws:sqs:us-east-1:123456789101:qdone_testQueue_failed',
          ReceiveMessageWaitTimeSeconds: '0',
          VisibilityTimeout: '30'
        }
      })
      .on(SendMessageCommand)
      .resolvesOnce({
        MD5OfMessageBody: md5,
        MessageId: messageId
      })

    await expect(
      enqueue('testQueue', 'true', opt)
    ).resolves.toEqual({ MD5OfMessageBody: md5, MessageId: messageId })

    expect(sqsMock).toHaveReceivedNthCommandWith(1, GetQueueUrlCommand, { QueueName: 'qdone_testQueue' })
    expect(sqsMock).toHaveReceivedNthCommandWith(2, GetQueueUrlCommand, { QueueName: 'qdone_testQueue_failed' })
    expect(sqsMock).toHaveReceivedNthCommandWith(3, CreateQueueCommand, {
      QueueName: 'qdone_testQueue_failed',
      Attributes: {
        MessageRetentionPeriod: opt.messageRetentionPeriod + ''
      }
    })
    expect(sqsMock).toHaveReceivedNthCommandWith(4, GetQueueAttributesCommand, {
      QueueUrl: 'https://q.amazonaws.com/123456789101/qdone_testQueue_failed',
      AttributeNames: ['All']
    })
    expect(sqsMock).toHaveReceivedNthCommandWith(5, CreateQueueCommand, {
      QueueName: 'qdone_testQueue',
      Attributes: {
        MessageRetentionPeriod: opt.messageRetentionPeriod + '',
        RedrivePolicy: JSON.stringify({
          deadLetterTargetArn: 'arn:aws:sqs:us-east-1:123456789101:qdone_testQueue_failed',
          maxReceiveCount: '1'
        })
      }
    })
    expect(sqsMock).toHaveReceivedNthCommandWith(6, SendMessageCommand, {
      QueueUrl: 'https://q.amazonaws.com/123456789101/qdone_testQueue',
      MessageBody: 'true'
    })
  })

  test('should print traceback and exit 1 with error', async () => {
    const opt = getOptionsWithDefaults()
    const err = new Error('Queue cannot be created.')
    err.name = 'SomeOtherError'
    err.Code = 'AWS.SimpleQueueService.SomeOtherError'
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand, { QueueName: 'qdone_testQueue_failed' })
      .rejectsOnce(new QueueDoesNotExist())
      .resolvesOnce({ QueueUrl: 'https://q.amazonaws.com/123456789101/qdone_testQueue_failed' })
      .on(GetQueueUrlCommand, { QueueName: 'qdone_testQueue' })
      .rejectsOnce(new QueueDoesNotExist())
      .resolvesOnce({ QueueUrl: 'https://q.amazonaws.com/123456789101/qdone_testQueue' })
      .on(CreateQueueCommand)
      .rejects(err)
      .on(GetQueueAttributesCommand, { QueueUrl: 'https://q.amazonaws.com/123456789101/qdone_testQueue_failed' })
      .resolvesOnce({
        Attributes: {
          CreatedTimestamp: '1442426968',
          DelaySeconds: '0',
          LastModifiedTimestamp: '1442426968',
          MaximumMessageSize: '262144',
          MessageRetentionPeriod: '345600',
          QueueArn: 'arn:aws:sqs:us-east-1:123456789101:qdone_testQueue_failed',
          ReceiveMessageWaitTimeSeconds: '0',
          VisibilityTimeout: '30'
        }
      })
      .on(SendMessageCommand)
      .resolvesOnce({
        MD5OfMessageAttributes: '00484c68...59e48f06',
        MD5OfMessageBody: '51b0a325...39163aa0',
        MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE'
      })
    await expect(
      enqueue('testQueue', 'true', opt)
    ).rejects.toThrow('cannot be created')
  })
})

describe('enqueueBatch', () => {
  test('basic enqueueBatch works', async () => {
    const messageId = '1e0632f4-b9e8-4f5c-a8e2-3529af1a56d6'
    const opt = getOptionsWithDefaults({ prefix: '', uuidFunction: () => messageId })
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
      enqueueBatch(pairs, opt)
    ).resolves.toBe(2)
    expect(sqsMock)
      .toHaveReceivedNthCommandWith(1, GetQueueUrlCommand, { QueueName: qname })
    expect(sqsMock)
      .toHaveReceivedNthCommandWith(2, SendMessageBatchCommand, {
        QueueUrl: qrl,
        Entries: [
          { MessageBody: cmd, Id: '0' },
          { MessageBody: cmd, Id: '1' }
        ]
      })
  })

  test('test/fixtures/test-unique01-x24.batch', async () => {
    const messageId = '1e0632f4-b9e8-4f5c-a8e2-3529af1a56d6'
    const opt = getOptionsWithDefaults({ prefix: '', uuidFunction: () => messageId })
    const qname = 'test'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const md5 = 'foobar'

    const sqsMock = mockClient(client)
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

    const pairs = await loadBatchFiles(['test/fixtures/test-unique01-x24.batch'])
    expect(pairs).toEqual(Array(24).fill({ command: 'true', queue: 'test' }))

    await expect(enqueueBatch(pairs, opt)).resolves.toBe(24)

    expect(sqsMock).toHaveReceivedCommandTimes(GetQueueUrlCommand, 1)
    expect(sqsMock).toHaveReceivedCommandTimes(SendMessageBatchCommand, 3)
  })

  test('test/fixtures/test-unique01-x24.batch with single group-id', async () => {
    const messageId = '1e0632f4-b9e8-4f5c-a8e2-3529af1a56d6'
    const opt = getOptionsWithDefaults({ prefix: '', fifo: true, groupId: 1, uuidFunction: () => messageId })
    const qname = 'test.fifo'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const md5 = 'foobar'

    const sqsMock = mockClient(client)
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

    const pairs = await loadBatchFiles(['test/fixtures/test-unique01-x24.batch'])
    expect(pairs).toEqual(Array(24).fill({ command: 'true', queue: 'test' }))

    await expect(enqueueBatch(pairs, opt)).resolves.toBe(24)

    expect(sqsMock).toHaveReceivedCommandTimes(GetQueueUrlCommand, 1)
    expect(sqsMock).toHaveReceivedCommandTimes(SendMessageBatchCommand, 3)
  })

  test('test/fixtures/test-unique01-x24.batch with unique group-id per message', async () => {
    const messageId = '1e0632f4-b9e8-4f5c-a8e2-3529af1a56d6'
    const opt = getOptionsWithDefaults({ prefix: '', fifo: true, groupIdPerMessage: true, uuidFunction: () => messageId })
    const qname = 'test.fifo'
    const qrl = `https://sqs.us-east-1.amazonaws.com/foobar/${qname}`
    const md5 = 'foobar'

    const sqsMock = mockClient(client)
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

    const pairs = await loadBatchFiles(['test/fixtures/test-unique01-x24.batch'])
    expect(pairs).toEqual(Array(24).fill({ command: 'true', queue: 'test' }))

    await expect(enqueueBatch(pairs, opt)).resolves.toBe(24)

    expect(sqsMock).toHaveReceivedCommandTimes(GetQueueUrlCommand, 1)
    expect(sqsMock).toHaveReceivedCommandTimes(SendMessageBatchCommand, 3)
  })

  test('should fail if requests share message buffers (bug suredone/suredone#9742)', async () => {
    const messageId = '1e0632f4-b9e8-4f5c-a8e2-3529af1a56d6'
    const opt = getOptionsWithDefaults({
      prefix: '',
      uuidFunction: () => messageId,
      groupId: messageId,
      deduplicationId: messageId
    })
    const qname1 = 'one'
    const qname2 = 'two'
    const qname3 = 'three.fifo'
    const qrlBase = 'https://sqs.us-east-1.amazonaws.com/foobar/'
    const qrl1 = qrlBase + qname1
    const qrl2 = qrlBase + qname2
    const qrl3 = qrlBase + qname3
    const cmd = 'sd BulkStatusModel finalizeAll'
    const sqsMock = mockClient(client)
    const md5 = 'foobar'
    const pairs = [
      { queue: qname1, command: cmd },
      { queue: qname1, command: cmd },
      { queue: qname1, command: cmd },
      { queue: qname1, command: cmd },
      { queue: qname1, command: cmd },
      { queue: qname1, command: cmd },
      { queue: qname1, command: cmd },
      { queue: qname1, command: cmd },
      { queue: qname1, command: cmd },
      { queue: qname1, command: cmd },
      { queue: qname2, command: cmd },
      { queue: qname2, command: cmd },
      { queue: qname2, command: cmd },
      { queue: qname2, command: cmd },
      { queue: qname2, command: cmd }
    ]
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand)
      .callsFake(({ QueueName: qname }) => {
        // console.log({ GetQueueUrlCommand: { QueueName: qname } })
        return { QueueUrl: qrlBase + qname }
      })
      .on(SendMessageBatchCommand)
      .callsFake(async ({ Entries: entries, QueueUrl: qrl }) => {
        // console.log({ SendMessageBatchCommand: { Entries: entries,  QueueUrl: qrl } })
        // console.log(entries)
        return {
          Successful: entries
            .filter(({ MessageGroupId, MessageDeduplicationId }) =>
              !(!qrl.endsWith('.fifo') && (MessageGroupId || MessageDeduplicationId))
            )
            .map(({ Id }) => ({
              Id,
              MD5OfMessageBody: md5,
              ...qrl.endsWith('.fifo')
                ? {
                    MessageId: messageId,
                    SequenceNumber: '110101010101' + Id
                  }
                : {}
            })),
          Failed: entries
            .filter(({ MessageGroupId, MessageDeduplicationId }) =>
              !qrl.endsWith('.fifo') && (MessageGroupId || MessageDeduplicationId)
            )
            .map(({ Id }) => ({
              Id, SenderFault: true, Code: 'InvalidParameterValue', Message: 'The request include parameter that is not valid for this queue type'
            }))
        }
      })
    const first = enqueueBatch(pairs, opt)
    const second = enqueueBatch(pairs.map(p => ({ queue: qname3, command: cmd })), { ...opt, fifo: true })
    await expect(first).resolves.toBe(15)
    await expect(second).resolves.toBe(15)
    expect(sqsMock).toHaveReceivedNthCommandWith(1, GetQueueUrlCommand, { QueueName: qname1 })
    expect(sqsMock).toHaveReceivedNthCommandWith(2, GetQueueUrlCommand, { QueueName: qname2 })
    expect(sqsMock).toHaveReceivedNthCommandWith(3, GetQueueUrlCommand, { QueueName: qname3 })
    expect(sqsMock)
      .toHaveReceivedNthCommandWith(4, SendMessageBatchCommand, {
        QueueUrl: qrl1,
        Entries: (Array.apply(null, Array(10))).map((e, i) => (
          { MessageBody: cmd, Id: i + '' }
        ))
      })
    expect(sqsMock)
      .toHaveReceivedNthCommandWith(5, SendMessageBatchCommand, {
        QueueUrl: qrl3,
        Entries: (Array.apply(null, Array(10))).map((e, i) => (
          { MessageBody: cmd, Id: i + '', MessageDeduplicationId: messageId, MessageGroupId: messageId }
        ))
      })
    expect(sqsMock)
      .toHaveReceivedNthCommandWith(6, SendMessageBatchCommand, {
        QueueUrl: qrl2,
        Entries: (Array.apply(null, Array(5))).map((e, i) => (
          { MessageBody: cmd, Id: 10 + i + '' }
        ))
      })
    expect(sqsMock)
      .toHaveReceivedNthCommandWith(7, SendMessageBatchCommand, {
        QueueUrl: qrl3,
        Entries: (Array.apply(null, Array(5))).map((e, i) => (
          { MessageBody: cmd, Id: 10 + i + '', MessageDeduplicationId: messageId, MessageGroupId: messageId }
        ))
      })
  })
})