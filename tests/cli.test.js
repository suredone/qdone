import { jest } from '@jest/globals'
import { readFileSync } from 'node:fs'
import { mockClient } from 'aws-sdk-client-mock'
import 'aws-sdk-client-mock-jest'

import {
  // ListQueuesCommand,
  GetQueueUrlCommand,
  GetQueueAttributesCommand,
  SendMessageCommand,
  CreateQueueCommand
} from '@aws-sdk/client-sqs'

import { run } from '../src/cli.js'
import { qrlCacheClear } from '../src/qrlCache.js'
import { getSQSClient, setSQSClient } from '../src/sqs.js'
import { getOptionsWithDefaults } from '../src/defaults.js'

delete process.env.AWS_ACCESS_KEY_ID
delete process.env.AWS_SECRET_ACCESS_KEY

const packageJson = JSON.parse(readFileSync('./package.json'))
getSQSClient()
const client = getSQSClient()

function spyConsole () {
  const spy = {}
  beforeEach(() => {
    spy.log = jest.spyOn(console, 'log')// .mockImplementation(() => {})
    spy.error = jest.spyOn(console, 'error')// .mockImplementation(() => {})
    if (!process.env.DEBUG) {
      spy.log.mockImplementation(() => {})
      spy.error.mockImplementation(() => {})
    }
  })
  afterEach(() => {
    spy.log.mockClear()
    spy.error.mockClear()
  })
  afterAll(() => {
    spy.log.mockRestore()
    spy.error.mockRestore()
  })
  return spy
}

const spy = spyConsole()
// Always clear qrl cache at the beginning of each test
beforeEach(qrlCacheClear)

// Root command
describe('qdone', () => {
  test('should print usage and exit 0', async () => {
    await run([])
    expect(console.log).toHaveBeenCalledTimes(1)
    expect(spy.log.mock.calls.join()).toContain('usage: ')
  })
})

describe('qdone --help', function () {
  test('should print usage and exit 0', async () => {
    await run(['--help'])
    expect(console.log).toHaveBeenCalledTimes(1)
    expect(spy.log.mock.calls.join()).toContain('usage: ')
  })
})

describe('qdone --version', function () {
  test('should print package.json version and exit 0', async () => {
    await run(['--version'])
    expect(console.log).toHaveBeenCalledTimes(1)
    expect(spy.log.mock.calls.join()).toContain(packageJson.version)
  })
})

describe('qdone --some-invalid-option', function () {
  test('should print usage and exit 1', async () => {
    await expect(run(['--some-invalid-option'])).rejects.toThrow('Unknown option')
  })
})

// Enqueue
describe('qdone enqueue', function () {
  test('should print usage and exit 1 with error', async () => {
    await expect(run(['enqueue', '--verbose'])).rejects.toThrow('')
    expect(spy.log.mock.calls.join()).toContain('usage: ')
  })
})

describe('qdone enqueue --help', function () {
  test('should print usage and exit 0', async () => {
    await expect(run(['enqueue', '--help']))
    expect(spy.log.mock.calls.join()).toContain('usage: ')
    expect(spy.log.mock.calls.join()).toContain('enqueue')
  })
})

describe('qdone enqueue onlyQueue', function () {
  test('should print usage and exit 1 with error', async () => {
    await expect(run(['enqueue', '--verbose', 'onlyQueue'])).rejects.toThrow('requires both')
    expect(spy.log.mock.calls.join()).toContain('usage: ')
    expect(spy.log.mock.calls.join()).toContain('<queue>')
  })
})

describe('qdone enqueue testQueue true # (with no credentials)', function () {
  test('should print usage and exit 1 with error', async () => {
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    const err = new Error('Access to the resource https://sqs.us-east-1.amazonaws.com/ is denied.')
    err.Code = 'AccessDenied'
    sqsMock.on(GetQueueUrlCommand).rejectsOnce(err)
    await expect(
      run(['enqueue', 'testQueue', 'true'])
    ).rejects.toThrow('Access to the resource')
    expect(spy.error.mock.calls.join()).toContain('Access to the resource https://sqs.us-east-1.amazonaws.com/ is denied.')
  })
})

describe('qdone enqueue testQueue true # (queue exists)', function () {
  test('should print id of enqueued message and exit 0', async () => {
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: 'https://q.amazonaws.com/123456789101/testQueue' })
      .on(SendMessageCommand)
      .resolvesOnce({
        MD5OfMessageAttributes: '00484c68...59e48f06',
        MD5OfMessageBody: '51b0a325...39163aa0',
        MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE'
      })
    await run(['enqueue', '--verbose', 'testQueue', 'true'])
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('da68f62c-0c07-4bee-bf5f-7e856EXAMPLE')
  })
})

describe('qdone enqueue --fifo testQueue true # (queue exists, fifo mode)', function () {
  test('should print id of enqueued message and exit 0', async () => {
    const opt = getOptionsWithDefaults({ fifo: true })
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: 'https://q.amazonaws.com/123456789101/qdone_testQueue.fifo' })
      .on(SendMessageCommand)
      .resolvesOnce({
        MD5OfMessageAttributes: '00484c68...59e48f06',
        MD5OfMessageBody: '51b0a325...39163aa0',
        MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE'
      })
    await run(['enqueue', '--verbose', '--fifo', 'testQueue', 'true'])
    expect(sqsMock).toHaveReceivedNthCommandWith(1, GetQueueUrlCommand, { QueueName: 'qdone_testQueue.fifo' })
    expect(sqsMock).toHaveReceivedNthCommandWith(2, SendMessageCommand, {
      QueueUrl: 'https://q.amazonaws.com/123456789101/qdone_testQueue.fifo',
      MessageBody: 'true',
      MessageDeduplicationId: opt.deduplicationId,
      MessageGroupId: opt.groupId
    })
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('da68f62c-0c07-4bee-bf5f-7e856EXAMPLE')
  })
})

describe('qdone enqueue --fifo --group-id gidtest testQueue true # (queue exists, fifo mode, explicit group)', function () {
  test('should print id of enqueued message and exit 0', async () => {
    const opt = getOptionsWithDefaults({ fifo: true })
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: 'https://q.amazonaws.com/123456789101/qdone_testQueue.fifo' })
      .on(SendMessageCommand)
      .resolvesOnce({
        MD5OfMessageAttributes: '00484c68...59e48f06',
        MD5OfMessageBody: '51b0a325...39163aa0',
        MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE'
      })
    await run(['enqueue', '--verbose', '--fifo', '--group-id', 'gidtest', 'testQueue', 'true'])
    expect(sqsMock).toHaveReceivedNthCommandWith(1, GetQueueUrlCommand, { QueueName: 'qdone_testQueue.fifo' })
    expect(sqsMock).toHaveReceivedNthCommandWith(2, SendMessageCommand, {
      QueueUrl: 'https://q.amazonaws.com/123456789101/qdone_testQueue.fifo',
      MessageBody: 'true',
      MessageDeduplicationId: opt.deduplicationId,
      MessageGroupId: 'gidtest'
    })
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('da68f62c-0c07-4bee-bf5f-7e856EXAMPLE')
  })
})

describe('qdone enqueue --quiet testQueue true # (queue exists)', function () {
  test('should have no output and exit 0', async () => {
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: 'https://q.amazonaws.com/123456789101/qdone_testQueue.fifo' })
      .on(SendMessageCommand)
      .resolvesOnce({
        MD5OfMessageAttributes: '00484c68...59e48f06',
        MD5OfMessageBody: '51b0a325...39163aa0',
        MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE'
      })
    await run(['enqueue', '--quiet', 'testQueue', 'true'])
    const stderr = spy.error.mock.calls.join()
    const stdout = spy.log.mock.calls.join()
    expect(stderr).toEqual('')
    expect(stdout).toEqual('')
  })
})

describe('qdone enqueue testQueue true # (queue does not exist)', function () {
  const opt = getOptionsWithDefaults()
  let sqsMock
  beforeEach(() => {
    const err = new Error('Queue does not exist.')
    err.name = 'QueueDoesNotExist'
    sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand, { QueueName: 'qdone_testQueue_failed' })
      .rejectsOnce(err)
      .resolvesOnce({ QueueUrl: 'https://q.amazonaws.com/123456789101/qdone_testQueue_failed' })
      .on(GetQueueUrlCommand, { QueueName: 'qdone_testQueue' })
      .rejectsOnce(err)
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
        MD5OfMessageAttributes: '00484c68...59e48f06',
        MD5OfMessageBody: '51b0a325...39163aa0',
        MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE'
      })
  })

  afterEach(() => {
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

  test('--verbose should create queues, print the id of enqueued message and exit 0', async () => {
    await run(['enqueue', '--verbose', 'testQueue', 'true'])
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('da68f62c-0c07-4bee-bf5f-7e856EXAMPLE')
    expect(stderr).toContain('Creating fail queue', 'testQueue_failed')
    expect(stderr).toContain('Creating queue', 'testQueue')
    // expect(stderr).toContain('Creating dead letter queue', 'testQueue_dead')
  })

  test('--quiet should create queues, print nothing and exit 0', async () => {
    await run(['enqueue', '--quiet', 'testQueue', 'true'])
    const stderr = spy.error.mock.calls.join()
    const stdout = spy.log.mock.calls.join()
    expect(stderr).toBe('')
    expect(stdout).toBe('')
  })
})

describe('qdone enqueue testQueue true # (unhandled error on fail queue creation)', function () {
  test('should print traceback and exit 1 with error', async () => {
    const err = new Error('Queue cannot be created.')
    err.name = 'SomeOtherError'
    err.Code = 'AWS.SimpleQueueService.SomeOtherError'
    const sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock
      .on(GetQueueUrlCommand, { QueueName: 'qdone_testQueue_failed' })
      .rejectsOnce(err)
      .resolvesOnce({ QueueUrl: 'https://q.amazonaws.com/123456789101/qdone_testQueue_failed' })
      .on(GetQueueUrlCommand, { QueueName: 'qdone_testQueue' })
      .rejectsOnce(err)
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
      run(['enqueue', '--verbose', 'testQueue', 'true'])
    ).rejects.toThrow('cannot be created')
  })
})

/*
// Enqueue batch
describe('qdone enqueue-batch', function () {
  test('should print usage and exit 1 with error', async () => {
    await expect(
      run(['enqueue-batch'])
    ).resolves.toEqual()
    const stdout = spy.log.mock.calls.join()
    expect(stdout).toContain('usage: ')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('<file>')
      expect(err).to.be.an('error')
    }))
})

describe('qdone enqueue-batch --help', function () {
  test('should print usage and exit 0', async () => {
    await expect(
      run(['enqueue-batch', '--help'])
    ).resolves.toEqual()
    const stdout = spy.log.mock.calls.join()
    expect(stdout).toContain('usage: ')
    const stdout = spy.log.mock.calls.join()
    expect(stdout).toContain('enqueue-batch')
    }))
})

describe('qdone enqueue-batch some_non_existent_file', function () {
  test('should exit 1 with error', async () => {
    await expect(
      run(['enqueue-batch', 'some_non_existent_file'])
    ).resolves.toEqual()
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('no such file or directory')
      expect(err).to.be.an('error')
    }))
})

describe('qdone enqueue-batch test/fixtures/test-unique01-x24.batch # (with no credentials)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      const err = new Error('Access to the resource https://sqs.us-east-1.amazonaws.com/ is denied.')
      err.code = 'AccessDenied'
      callback(err)
    })
  })
  test('should print usage and exit 1 with error', async () => {
    await expect(
      run(['enqueue-batch', 'test/fixtures/test-unique01-x24.batch'])
    ).resolves.toEqual()
    const stdout = spy.log.mock.calls.join()
    expect(stdout).toContain('You must provide')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Access to the resource https://sqs.us-east-1.amazonaws.com/ is denied.')
      expect(err).to.be.an('error')
    }))
})

describe('qdone enqueue-batch test/fixtures/test-unique01-x24.batch # (queue exists)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    var messageId = 0
    sqsMock
      .on('sendMessageBatch')
      .resolvesOnce({
        Failed: [],
        Successful: params.Entries.map(message => ({
          MD5OfMessageAttributes: '00484c68...59e48f06',
          MD5OfMessageBody: '51b0a325...39163aa0',
          MessageId: 'da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId++
        }))
      })
    })
  })
  test('should print id of enqueued messages, use 3 requests, print total count and exit 0', async () => {
    await expect(
      run(['enqueue-batch', 'test/fixtures/test-unique01-x24.batch'])
    ).resolves.toEqual()
      for (var messageId = 0; messageId < 24; messageId++) {
      const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Enqueued job da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId)
      }
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Enqueued 24 jobs')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('request 1')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('request 2')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('request 3')
    }))
})

describe('qdone enqueue-batch --fifo --group-id-per-message test/fixtures/test-unique01-x24.batch # (queue exists, group ids should be unique)', function () {
  let groupIds
  beforeAll(function () {
    groupIds = {}
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    var messageId = 0
    sqsMock
      .on('sendMessageBatch')
      params.Entries.forEach(message => {
        groupIds[message.MessageGroupId] = true
      })
      .resolvesOnce({
        Failed: [],
        Successful: params.Entries.map(message => ({
          MD5OfMessageAttributes: '00484c68...59e48f06',
          MD5OfMessageBody: '51b0a325...39163aa0',
          MessageId: 'da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId++
        }))
      })
    })
  })
  test('should print id of enqueued messages, use 3 requests, use unique group ids for every message, print total count and exit 0', async () => {
    await expect(
      run(['enqueue-batch', '--fifo', '--group-id-per-message', 'test/fixtures/test-unique01-x24.batch'])
    ).resolves.toEqual()
      for (var messageId = 0; messageId < 24; messageId++) {
      const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Enqueued job da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId)
      }
      expect(Object.keys(groupIds).length).to.equal(24)
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Enqueued 24 jobs')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('request 1')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('request 2')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('request 3')
    }))
})

describe('qdone enqueue-batch test/fixtures/test-unique01-x24.batch # (queue exists, some failures)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    var messageId = 0
    sqsMock
      .on('sendMessageBatch')
      .resolvesOnce({
        Failed: params.Entries.slice(0, 2).map(message => ({
          MD5OfMessageAttributes: '00484c68...59e48f06',
          MD5OfMessageBody: '51b0a325...39163aa0',
          MessageId: 'da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId++
        })),
        Successful: params.Entries.slice(2).map(message => ({
          MD5OfMessageAttributes: '00484c68...59e48f06',
          MD5OfMessageBody: '51b0a325...39163aa0',
          MessageId: 'da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId++
        }))
      })
    })
  })
  test('should exit 1 and show which messages failed', async () => {
    await expect(
      run(['enqueue-batch', '--verbose', 'test/fixtures/test-unique01-x24.batch'])
    ).resolves.toEqual()
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Error: One or more message failures')
      expect(err).to.be.an('error')
      // Expect some ids of failed messages
      for (var messageId = 0; messageId < 2; messageId++) {
      const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId)
      }
    }))
})

describe('qdone enqueue-batch --quiet test/fixtures/test-unique01-x24.batch # (queue exists)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    var messageId = 0
    sqsMock
      .on('sendMessageBatch')
      .resolvesOnce({
        Failed: [],
        Successful: params.Entries.map(message => ({
          MD5OfMessageAttributes: '00484c68...59e48f06',
          MD5OfMessageBody: '51b0a325...39163aa0',
          MessageId: 'da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId++
        }))
      })
    })
  })
  test('should have no output and exit 0', async () => {
    await expect(
      run(['enqueue-batch', '--quiet', 'test/fixtures/test-unique01-x24.batch'])
    ).resolves.toEqual()
      expect(stderr).to.equal('')
      expect(stdout).to.equal('')
    }))
})

describe('qdone enqueue-batch --fifo test/fixtures/test-fifo01-x24.batch # (queue does not exist)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      const err = new Error('Queue does not exist.')
      err.code = 'AWS.SimpleQueueService.NonExistentQueue'
      callback(err)
    })
    sqsMock
      .on(CreateQueueCommand)
      expect(params.QueueName.slice(-'.fifo'.length) === '.fifo')
      expect(params.FifoQueue === 'true')
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    sqsMock
      .on(GetQueueAttributesCommand)
      .resolvesOnce({
        Attributes: {
          ApproximateNumberOfMessages: '0',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0',
          CreatedTimestamp: '1442426968',
          DelaySeconds: '0',
          LastModifiedTimestamp: '1442426968',
          MaximumMessageSize: '262144',
          MessageRetentionPeriod: '345600',
          QueueArn: 'arn:aws:sqs:us-east-1:80398EXAMPLE:MyNewQueue',
          ReceiveMessageWaitTimeSeconds: '0',
          RedrivePolicy: `{'deadLetterTargetArn':'arn:aws:sqs:us-east-1:80398EXAMPLE:${params.QueueName}','maxReceiveCount':1000}`,
          VisibilityTimeout: '30'
        }
      })
    })
    var messageId = 0
    sqsMock
      .on('sendMessageBatch')
      .resolvesOnce({
        Failed: [],
        Successful: params.Entries.map(message => ({
          MD5OfMessageAttributes: '00484c68...59e48f06',
          MD5OfMessageBody: '51b0a325...39163aa0',
          MessageId: 'da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId++
        }))
      })
    })
  })
  test('should print id of enqueued messages, use 3 requests, print total count and exit 0', async () => {
    await expect(
      run(['enqueue-batch', '--verbose', '--fifo', 'test/fixtures/test-fifo01-x24.batch'])
    ).resolves.toEqual()
      for (var messageId = 0; messageId < 24; messageId++) {
      const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Enqueued job da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId)
      }
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Enqueued 24 jobs')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('request 1')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('request 2')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('request 3')
      expect(stderr).to.not.contain('request 4')
    }))
})

describe('qdone enqueue-batch test/fixtures/test-unique01-x24.batch # (queue does not exist)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      const err = new Error('Queue does not exist.')
      err.code = 'AWS.SimpleQueueService.NonExistentQueue'
      callback(err)
    })
    sqsMock
      .on(CreateQueueCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    sqsMock
      .on(GetQueueAttributesCommand)
      .resolvesOnce({
        Attributes: {
          ApproximateNumberOfMessages: '0',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0',
          CreatedTimestamp: '1442426968',
          DelaySeconds: '0',
          LastModifiedTimestamp: '1442426968',
          MaximumMessageSize: '262144',
          MessageRetentionPeriod: '345600',
          QueueArn: 'arn:aws:sqs:us-east-1:80398EXAMPLE:MyNewQueue',
          ReceiveMessageWaitTimeSeconds: '0',
          RedrivePolicy: `{'deadLetterTargetArn':'arn:aws:sqs:us-east-1:80398EXAMPLE:${params.QueueName}','maxReceiveCount':1000}`,
          VisibilityTimeout: '30'
        }
      })
    })
    var messageId = 0
    sqsMock
      .on('sendMessageBatch')
      .resolvesOnce({
        Failed: [],
        Successful: params.Entries.map(message => ({
          MD5OfMessageAttributes: '00484c68...59e48f06',
          MD5OfMessageBody: '51b0a325...39163aa0',
          MessageId: 'da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId++
        }))
      })
    })
  })
  test('should create queues, print ids of enqueued messages, use 3 requests, print total count and exit 0', async () => {
    await expect(
      run(['enqueue-batch', 'test/fixtures/test-unique01-x24.batch'])
    ).resolves.toEqual()
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Creating fail queue test_failed')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Creating queue test')
      for (var messageId = 0; messageId < 24; messageId++) {
      const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Enqueued job da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId)
      }
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Enqueued 24 jobs')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('request 1')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('request 2')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('request 3')
    }))
})

describe('qdone enqueue-batch test/fixtures/test-unique{01-x24.batch,02-x24.batch,24-x24.batch,24-x240.batch} # (ensemble fixtures, queue exists)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    var messageId = 0
    sqsMock
      .on('sendMessageBatch')
      .resolvesOnce({
        Failed: [],
        Successful: params.Entries.map(message => ({
          MD5OfMessageAttributes: '00484c68...59e48f06',
          MD5OfMessageBody: '51b0a325...39163aa0',
          MessageId: 'da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId++
        }))
      })
    })
  })
  test('should print id of enqueued messages, use 53 requests, print total count and exit 0', async () => {
    cliTest([
      'enqueue-batch',
      'test/fixtures/test-unique01-x24.batch',
      'test/fixtures/test-unique02-x24.batch',
      'test/fixtures/test-unique24-x24.batch',
      'test/fixtures/test-unique24-x240.batch'
    ], function (result, stdout, stderr) {
      for (var messageId = 0; messageId < 312; messageId++) {
      const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Enqueued job da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId)
      }
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Enqueued 312 jobs')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('request 1')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('request 2')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('request 53')
    }))
})

describe('qdone enqueue-batch test/fixtures/test-too-big-1.batch # (messages too big for full batch)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      const err = new Error('Queue does not exist.')
      err.code = 'AWS.SimpleQueueService.NonExistentQueue'
      callback(err)
    })
    sqsMock
      .on(CreateQueueCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    sqsMock
      .on(GetQueueAttributesCommand)
      .resolvesOnce({
        Attributes: {
          ApproximateNumberOfMessages: '0',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0',
          CreatedTimestamp: '1442426968',
          DelaySeconds: '0',
          LastModifiedTimestamp: '1442426968',
          MaximumMessageSize: '262144',
          MessageRetentionPeriod: '345600',
          QueueArn: 'arn:aws:sqs:us-east-1:80398EXAMPLE:MyNewQueue',
          ReceiveMessageWaitTimeSeconds: '0',
          RedrivePolicy: `{'deadLetterTargetArn':'arn:aws:sqs:us-east-1:80398EXAMPLE:${params.QueueName}','maxReceiveCount':1000}`,
          VisibilityTimeout: '30'
        }
      })
    })
    var messageId = 0
    sqsMock
      .on('sendMessageBatch')
      .resolvesOnce({
        Failed: [],
        Successful: params.Entries.map(message => ({
          MD5OfMessageAttributes: '00484c68...59e48f06',
          MD5OfMessageBody: '51b0a325...39163aa0',
          MessageId: 'da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId++
        }))
      })
    })
  })
  test('should print ids of enqueued messages, use 5 requests, print total count and exit 0', async () => {
    await expect(
      run(['enqueue-batch', 'test/fixtures/test-too-big-1.batch'])
    ).resolves.toEqual()
      for (var messageId = 0; messageId < 10; messageId++) {
      const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Enqueued job da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId)
      }
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Enqueued 10 jobs')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('request 1')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('request 2')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('request 3')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('request 4')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('request 5')
    }))
})

// Worker
describe('qdone worker', function () {
  test('should print usage and exit 1 with error', async () => {
    await expect(
      run(['worker'])
    ).resolves.toEqual()
    const stdout = spy.log.mock.calls.join()
    expect(stdout).toContain('usage: ')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('<queue>')
      expect(err).to.be.an('error')
    }))
})

describe('qdone worker --help', function () {
  test('should print usage and exit 0', async () => {
    await expect(
      run(['worker', '--help'])
    ).resolves.toEqual()
    const stdout = spy.log.mock.calls.join()
    expect(stdout).toContain('usage: ')
    const stdout = spy.log.mock.calls.join()
    expect(stdout).toContain('worker')
    }))
})

describe('qdone worker some_non_existent_queue --drain', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      const err = new Error('Queue does not exist.')
      err.code = 'AWS.SimpleQueueService.NonExistentQueue'
      callback(err)
    })
  })
  test('should complain and exit 0', async () => {
    await expect(
      run(['worker', 'some_non_existent_queue', '--drain'], null)
    ).resolves.toEqual()
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('AWS.SimpleQueueService.NonExistentQueue')
    }))
})

describe('qdone worker test --drain # (no jobs)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    sqsMock
      .on('listQueues')
      .resolvesOnce({ QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    sqsMock
      .on('receiveMessage')
      .resolvesOnce({})
    })
    sqsMock
      .on('deleteMessage')
      .resolvesOnce({})
    })
  })
  test('should execute the job successfully and exit 0', async () => {
    await expect(
      run(['worker', 'test', '--drain'])
    ).resolves.toEqual()
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Looking for work on test')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Ran 0 jobs: 0 succeeded 0 failed')
    }))
})

describe('qdone worker test --drain # (1 successful job)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    sqsMock
      .on('listQueues')
      .resolvesOnce({ QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    sqsMock
      .on('receiveMessage')
      .resolvesOnce({ Messages: [
        { MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE', Body: 'true', ReceiptHandle: 'AQEBzbVv...fqNzFw==' }
      ] })
      AWS.restore('SQS', 'receiveMessage')
      // Subsequent calls return no message
      sqsMock
      .on('receiveMessage')
        .resolvesOnce({})
      })
    })
    sqsMock
      .on('deleteMessage')
      .resolvesOnce({})
    })
  })
  test('should execute the job successfully and exit 0', async () => {
    await expect(
      run(['worker', 'test', '--drain'])
    ).resolves.toEqual()
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Looking for work on test')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Found job da68f62c-0c07-4bee-bf5f-7e856EXAMPLE')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('SUCCESS')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Ran 1 jobs: 1 succeeded 0 failed')
    }))
})

describe('qdone worker test --drain --quiet # (1 failed job)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    sqsMock
      .on('listQueues')
      .resolvesOnce({ QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    sqsMock
      .on('receiveMessage')
      .resolvesOnce({ Messages: [
        { MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE', Body: 'false', ReceiptHandle: 'AQEBzbVv...fqNzFw==' }
      ] })
      AWS.restore('SQS', 'receiveMessage')
      // Subsequent calls return no message
      sqsMock
      .on('receiveMessage')
        .resolvesOnce({})
      })
    })
    sqsMock
      .on('deleteMessage')
      .resolvesOnce({})
    })
  })
  test('should execute the job successfully and exit 0', async () => {
    await expect(
      run(['worker', 'test', '--drain', '--quiet'])
    ).resolves.toEqual()
    const stdout = spy.log.mock.calls.join()
    expect(stdout).toContain('"event":"JOB_FAILED"')
    const stdout = spy.log.mock.calls.join()
    expect(stdout).toContain('"command":"false"')
    const stdout = spy.log.mock.calls.join()
    expect(stdout).toContain('"timestamp"')
    const stdout = spy.log.mock.calls.join()
    expect(stdout).toContain('"job"')
    const stdout = spy.log.mock.calls.join()
    expect(stdout).toContain('"exitCode"')
    }))
})

describe('qdone worker test --drain --kill-after 1 --wait-time 1 --quiet # (job runs past kill timer)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    sqsMock
      .on('listQueues')
      .resolvesOnce({ QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    sqsMock
      .on('receiveMessage')
      .resolvesOnce({ Messages: [
        { MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE', Body: 'bash test/fixtures/test-child-kill-linux.sh', ReceiptHandle: 'AQEBzbVv...fqNzFw==' }
      ] })
      AWS.restore('SQS', 'receiveMessage')
      // Subsequent calls return no message
      sqsMock
      .on('receiveMessage')
        .resolvesOnce({})
      })
      process.nextTick(function () {
        clock.tick(1500)
      })
    })
    sqsMock
      .on('deleteMessage')
      .resolvesOnce({})
    })
  })
  test('should execute the job successfully and exit 0', async () => {
    await expect(
      run(['worker', 'test', '--drain', '--kill-after', '1', '--wait-time', '1'])
    ).resolves.toEqual()
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('FAILED')
      // Check that file does not continue to be written to
      expect(fs.readFileSync('/tmp/qdone-test-child-kill-linux.out').toString()).to.equal('terminated\n')
    }))
})

describe('qdone worker "test*" --drain # (9 queues, 1 successful job per queue)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    sqsMock
      .on('listQueues')
      .resolvesOnce({ QueueUrls: [
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}1`,
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}2`,
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}3`,
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}4`,
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}5`,
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}6`,
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}7`,
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}8`,
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}9`
      ] })
    })
    sqsMock
      .on('receiveMessage')
      .resolvesOnce({ Messages: [
        { MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE-' + params.QueueUrl.slice(-1), Body: 'true', ReceiptHandle: 'AQEBzbVv...fqNzFw==' }
      ] })
      if (params.QueueUrl === params.QueueUrl.slice(0, -1) + '9') {
        AWS.restore('SQS', 'receiveMessage')
        // Subsequent calls return no message
        sqsMock
      .on('receiveMessage')
          .resolvesOnce({})
        })
      }
    })
    sqsMock
      .on('deleteMessage')
      .resolvesOnce({})
    })
  })
  test('should execute the job successfully and exit 0', async () => {
    await expect(
      run(['worker', 'test*', '--drain'])
    ).resolves.toEqual()
      [1, 2, 3, 4, 5, 5, 6, 7, 8, 9].forEach(index => {
      const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Looking for work on test' + index)
      const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Found job da68f62c-0c07-4bee-bf5f-7e856EXAMPLE-' + index)
      })
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Ran 9 jobs: 9 succeeded 0 failed')
    }))
})

describe('qdone worker test --drain # (1 successful job, time extended)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    sqsMock
      .on('listQueues')
      .resolvesOnce({ QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    sqsMock
      .on('changeMessageVisibility')
      .resolvesOnce({})
    })
    sqsMock
      .on('receiveMessage')
      .resolvesOnce({ Messages: [
        { MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE', Body: 'sleep 1', ReceiptHandle: 'AQEBzbVv...fqNzFw==' }
      ] })
      AWS.restore('SQS', 'receiveMessage')
      // Subsequent calls return no message
      sqsMock
      .on('receiveMessage')
        .resolvesOnce({})
      })
      process.nextTick(function () {
        clock.tick(15000)
      })
    })
    sqsMock
      .on('deleteMessage')
      .resolvesOnce({})
    })
  })
  test('should execute the job successfully and exit 0', async () => {
    await expect(
      run(['worker', 'test', '--drain'])
    ).resolves.toEqual()
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Looking for work on test')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Found job da68f62c-0c07-4bee-bf5f-7e856EXAMPLE')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('seconds, requesting another')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('SUCCESS')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Ran 1 jobs: 1 succeeded 0 failed')
    }))
})

describe('qdone worker test --drain --quiet # (1 successful job, time extended)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    sqsMock
      .on('listQueues')
      .resolvesOnce({ QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    sqsMock
      .on('changeMessageVisibility')
      .resolvesOnce({})
    })
    sqsMock
      .on('receiveMessage')
      .resolvesOnce({ Messages: [
        { MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE', Body: 'sleep 1', ReceiptHandle: 'AQEBzbVv...fqNzFw==' }
      ] })
      AWS.restore('SQS', 'receiveMessage')
      // Subsequent calls return no message
      sqsMock
      .on('receiveMessage')
        .resolvesOnce({})
      })
      process.nextTick(function () {
        clock.tick(15000)
      })
    })
    sqsMock
      .on('deleteMessage')
      .resolvesOnce({})
    })
  })
  test('should have no output and exit 0', async () => {
    await expect(
      run(['worker', 'test', '--drain', '--quiet'])
    ).resolves.toEqual()
      expect(stderr).to.equal('')
      expect(stdout).to.equal('')
    }))
})

describe('qdone worker "test*" --drain --active-only # (4 queues, 2 full, 2 empty)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    sqsMock
      .on('listQueues')
      .resolvesOnce({ QueueUrls: [
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}1`,
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}2`,
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}3`,
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}4`
      ] })
    })
    sqsMock
      .on(GetQueueAttributesCommand)
      const lastLetter = params.QueueUrl.slice(-1)
      .resolvesOnce({
        Attributes: {
          ApproximateNumberOfMessages: (lastLetter === '1' || lastLetter === '2') ? '1' : '0',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0',
          CreatedTimestamp: '1442426968',
          DelaySeconds: '0',
          LastModifiedTimestamp: '1442426968',
          MaximumMessageSize: '262144',
          MessageRetentionPeriod: '345600',
          QueueArn: 'arn:aws:sqs:us-east-1:80398EXAMPLE:MyNewQueue',
          ReceiveMessageWaitTimeSeconds: '0',
          RedrivePolicy: `{'deadLetterTargetArn':'arn:aws:sqs:us-east-1:80398EXAMPLE:${params.QueueName}','maxReceiveCount':1000}`,
          VisibilityTimeout: '30'
        }
      })
    })
    sqsMock
      .on('receiveMessage')
      .resolvesOnce({ Messages: [
        { MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE-' + params.QueueUrl.slice(-1), Body: 'true', ReceiptHandle: 'AQEBzbVv...fqNzFw==' }
      ] })
      if (params.QueueUrl === params.QueueUrl.slice(0, -1) + '2') {
        AWS.restore('SQS', 'receiveMessage')
        // Subsequent calls return no message
        sqsMock
      .on('receiveMessage')
          .resolvesOnce({})
        })
      }
    })
    sqsMock
      .on('deleteMessage')
      .resolvesOnce({})
    })
  })
  test('should execute the job successfully and exit 0', async () => {
    await expect(
      run(['worker', 'test*', '--drain', '--active-only'])
    ).resolves.toEqual()
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Found job da68f62c-0c07-4bee-bf5f-7e856EXAMPLE-1')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Found job da68f62c-0c07-4bee-bf5f-7e856EXAMPLE-2')
      expect(stderr).to.not.contain('Found job da68f62c-0c07-4bee-bf5f-7e856EXAMPLE-3')
      expect(stderr).to.not.contain('Found job da68f62c-0c07-4bee-bf5f-7e856EXAMPLE-4')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Ran 2 jobs: 2 succeeded 0 failed')
    }))
})

describe('qdone worker test # (1 successful job + SIGTERM)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    sqsMock
      .on('listQueues')
      .resolvesOnce({ QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    sqsMock
      .on('receiveMessage')
      .resolvesOnce({ Messages: [
        { MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE', Body: 'sleep 1', ReceiptHandle: 'AQEBzbVv...fqNzFw==' }
      ] })
      AWS.restore('SQS', 'receiveMessage')
      // Subsequent calls return no message
      sqsMock
      .on('receiveMessage')
        .resolvesOnce({})
      })
      // And here we trigger a SIGTERM
      process.kill(process.pid, 'SIGTERM')
    })
    sqsMock
      .on('deleteMessage')
      .resolvesOnce({})
    })
  })
  test('should begin executing the job, acknowledge a SIGTERM and successfully and exit 0 after the job completes', async () => {
    await expect(
      run(['worker', 'test'])
    ).resolves.toEqual()
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Looking for work on test')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Found job da68f62c-0c07-4bee-bf5f-7e856EXAMPLE')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Shutdown requested')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('SUCCESS')
    }))
})

// Idle queues

describe('qdone idle-queues', function () {
  test('should print usage and exit 1 with error', async () => {
    await expect(
      run(['idle-queues'])
    ).resolves.toEqual()
    const stdout = spy.log.mock.calls.join()
    expect(stdout).toContain('usage: ')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('<queue>')
      expect(err).to.be.an('error')
    }))
})

describe('qdone idle-queues --help', function () {
  test('should print usage and exit 0', async () => {
    await expect(
      run(['idle-queues', '--help'])
    ).resolves.toEqual()
    const stdout = spy.log.mock.calls.join()
    expect(stdout).toContain('usage: ')
    const stdout = spy.log.mock.calls.join()
    expect(stdout).toContain('idle-queues')
    }))
})

describe('qdone idle-queues test --include-failed', function () {
  test('should print usage and note about --include-failed and exit 1 with error', async () => {
    await expect(
      run(['idle-queues', 'test', '--include-failed'])
    ).resolves.toEqual()
    const stdout = spy.log.mock.calls.join()
    expect(stdout).toContain('usage: ')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('--include-failed should be used with --unpair')
      expect(err).to.be.an('error')
    }))
})

describe('qdone idle-queues test # (active queue shortcut via SQS API)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    sqsMock
      .on('listQueues')
      .resolvesOnce({ QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    sqsMock
      .on(GetQueueAttributesCommand)
      .resolvesOnce({
        Attributes: {
          ApproximateNumberOfMessages: '1',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    })
  })
  test('should make no CloudWatch calls, print nothing to stdout and exit 0', async () => {
    await expect(
      run(['idle-queues', 'test'])
    ).resolves.toEqual()
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Queue test has been active in the last 60 minutes.')
      expect(stdout).to.equal('')
    }))
})

describe('qdone idle-queues test --unpair # (active queue shortcut via SQS API)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    sqsMock
      .on('listQueues')
      .resolvesOnce({ QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    sqsMock
      .on(GetQueueAttributesCommand)
      .resolvesOnce({
        Attributes: {
          ApproximateNumberOfMessages: '1',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    })
  })
  test('should make no CloudWatch calls, print nothing to stdout and exit 0', async () => {
    await expect(
      run(['idle-queues', 'test', '--unpair'])
    ).resolves.toEqual()
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Queue test has been active in the last 60 minutes.')
      expect(stdout).to.equal('')
    }))
})

describe('qdone idle-queues test --cache-uri redis://localhost # (cached getQueueAttributes call)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    sqsMock
      .on('listQueues')
      .resolvesOnce({ QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    sqsMock
      .on(GetQueueAttributesCommand)
      .resolvesOnce({
        Attributes: {
          ApproximateNumberOfMessages: '1',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    })
  })
  test('should make one call to getQueueAttributes, print nothing to stdout and exit 0', async () => {
    await expect(
      run(['idle-queues', 'test', '--cache-uri', 'redis://localhost'])
    ).resolves.toEqual()
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Queue test has been active in the last 60 minutes.')
      expect(stdout).to.equal('')
    }))
})

describe('qdone idle-queues test # (active queue, multiple CloudWatch calls)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    sqsMock
      .on('listQueues')
      .resolvesOnce({ QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    sqsMock
      .on(GetQueueAttributesCommand)
      .resolvesOnce({
        Attributes: {
          ApproximateNumberOfMessages: '0',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    })
    sqsMock
      .on('getMetricStatistics')
      if (params.MetricName === 'ApproximateNumberOfMessagesDelayed') {
        .resolvesOnce({
          Label: params.MetricName,
          Datapoints: [
            { Timestamp: new Date(), Sum: 0, Metric: 'Count' },
            { Timestamp: new Date(), Sum: 1, Metric: 'Count' }
          ]
        })
      } else {
        .resolvesOnce({
          Label: params.MetricName,
          Datapoints: [
            { Timestamp: new Date(), Sum: 0, Metric: 'Count' },
            { Timestamp: new Date(), Sum: 0, Metric: 'Count' }
          ]
        })
      }
    })
  })
  test('should make CloudWatch calls, print nothing to stdout and exit 0', async () => {
    await expect(
      run(['idle-queues', 'test'])
    ).resolves.toEqual()
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Queue test has been active in the last 60 minutes.')
      expect(stdout).to.equal('')
    }))
})

describe('qdone idle-queues test # (inactive queue)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    sqsMock
      .on('listQueues')
      .resolvesOnce({ QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    sqsMock
      .on(GetQueueAttributesCommand)
      .resolvesOnce({
        Attributes: {
          ApproximateNumberOfMessages: '0',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    })
    sqsMock
      .on('getMetricStatistics')
      // Always return 0s
      .resolvesOnce({
        Label: params.MetricName,
        Datapoints: [
          { Timestamp: new Date(), Sum: 0, Metric: 'Count' },
          { Timestamp: new Date(), Sum: 0, Metric: 'Count' }
        ]
      })
    })
  })
  test('should print queue name to stdout and exit 0', async () => {
    await expect(
      run(['idle-queues', 'test'])
    ).resolves.toEqual()
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Queue test has been idle for the last 60 minutes.')
    const stdout = spy.log.mock.calls.join()
    expect(stdout).toContain('test\n')
    }))
})

describe('qdone idle-queues test --delete # (inactive queue)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    sqsMock
      .on('listQueues')
      .resolvesOnce({ QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    sqsMock
      .on(GetQueueAttributesCommand)
      .resolvesOnce({
        Attributes: {
          ApproximateNumberOfMessages: '0',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    })
    sqsMock
      .on('getMetricStatistics')
      // Always return 0s
      .resolvesOnce({
        Label: params.MetricName,
        Datapoints: [
          { Timestamp: new Date(), Sum: 0, Metric: 'Count' },
          { Timestamp: new Date(), Sum: 0, Metric: 'Count' }
        ]
      })
    })
    sqsMock
      .on('deleteQueue')
      .resolvesOnce({})
    })
  })
  test('should print queue name to stdout and exit 0', async () => {
    await expect(
      run(['idle-queues', 'test', '--delete'])
    ).resolves.toEqual()
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Queue test has been idle for the last 60 minutes.')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Deleted test')
    const stdout = spy.log.mock.calls.join()
    expect(stdout).toContain('test\n')
    }))
})

describe('qdone idle-queues test --unpair # (inactive queue)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    sqsMock
      .on('listQueues')
      .resolvesOnce({ QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    sqsMock
      .on(GetQueueAttributesCommand)
      .resolvesOnce({
        Attributes: {
          ApproximateNumberOfMessages: '0',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    })
    sqsMock
      .on('getMetricStatistics')
      // Always return 0s
      .resolvesOnce({
        Label: params.MetricName,
        Datapoints: [
          { Timestamp: new Date(), Sum: 0, Metric: 'Count' },
          { Timestamp: new Date(), Sum: 0, Metric: 'Count' }
        ]
      })
    })
  })
  test('should print queue name to stdout and exit 0', async () => {
    await expect(
      run(['idle-queues', 'test', '--unpair'])
    ).resolves.toEqual()
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Queue test has been idle for the last 60 minutes.')
    const stdout = spy.log.mock.calls.join()
    expect(stdout).toContain('test\n')
    }))
})

describe('qdone idle-queues test --unpair --delete # (inactive queue)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    sqsMock
      .on('listQueues')
      .resolvesOnce({ QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    sqsMock
      .on(GetQueueAttributesCommand)
      .resolvesOnce({
        Attributes: {
          ApproximateNumberOfMessages: '0',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    })
    sqsMock
      .on('getMetricStatistics')
      // Always return 0s
      .resolvesOnce({
        Label: params.MetricName,
        Datapoints: [
          { Timestamp: new Date(), Sum: 0, Metric: 'Count' },
          { Timestamp: new Date(), Sum: 0, Metric: 'Count' }
        ]
      })
    })
    sqsMock
      .on('deleteQueue')
      .resolvesOnce({})
    })
  })
  test('should print queue name to stdout and exit 0', async () => {
    await expect(
      run(['idle-queues', 'test', '--unpair', '--delete'])
    ).resolves.toEqual()
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Queue test has been idle for the last 60 minutes.')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Deleted test')
    const stdout = spy.log.mock.calls.join()
    expect(stdout).toContain('test\n')
    }))
})

describe('qdone idle-queues test # (primary queue is idle, failed queue is active)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    sqsMock
      .on('listQueues')
      .resolvesOnce({ QueueUrls: [
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}`,
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}_failed`
      ] })
    })
    sqsMock
      .on(GetQueueAttributesCommand)
      .resolvesOnce({
        Attributes: {
          ApproximateNumberOfMessages: '0',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    })
    sqsMock
      .on('getMetricStatistics')
      // Always return data for failed queue
      if (params.Dimensions[0].Value === 'qdone_test_failed') {
        .resolvesOnce({
          Label: params.MetricName,
          Datapoints: [
            { Timestamp: new Date(), Sum: 0, Metric: 'Count' },
            { Timestamp: new Date(), Sum: 1, Metric: 'Count' }
          ]
        })
      } else {
        .resolvesOnce({
          Label: params.MetricName,
          Datapoints: [
            { Timestamp: new Date(), Sum: 0, Metric: 'Count' },
            { Timestamp: new Date(), Sum: 0, Metric: 'Count' }
          ]
        })
      }
    })
  })
  test('should print queue name to stdout and exit 0', async () => {
    await expect(
      run(['idle-queues', 'test'])
    ).resolves.toEqual()
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Queue test has been idle for the last 60 minutes.')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Queue test_failed has been active in the last 60 minutes.')
    }))
})

describe('qdone idle-queues test.fifo # (primary queue is idle and a FIFO, failed queue is active)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    sqsMock
      .on('listQueues')
      .resolvesOnce({ QueueUrls: [
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}`,
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}_failed.fifo`
      ] })
    })
    sqsMock
      .on(GetQueueAttributesCommand)
      .resolvesOnce({
        Attributes: {
          ApproximateNumberOfMessages: '0',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    })
    sqsMock
      .on('getMetricStatistics')
      // Always return data for failed queue
      if (params.Dimensions[0].Value === 'qdone_test_failed.fifo') {
        .resolvesOnce({
          Label: params.MetricName,
          Datapoints: [
            { Timestamp: new Date(), Sum: 0, Metric: 'Count' },
            { Timestamp: new Date(), Sum: 1, Metric: 'Count' }
          ]
        })
      } else {
        .resolvesOnce({
          Label: params.MetricName,
          Datapoints: [
            { Timestamp: new Date(), Sum: 0, Metric: 'Count' },
            { Timestamp: new Date(), Sum: 0, Metric: 'Count' }
          ]
        })
      }
    })
  })
  test('should print queue name to stdout and exit 0', async () => {
    await expect(
      run(['idle-queues', 'test.fifo'])
    ).resolves.toEqual()
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Queue test.fifo has been idle for the last 60 minutes.')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Queue test_failed.fifo has been active in the last 60 minutes.')
    }))
})

describe('qdone idle-queues \'test*\' --unpair --include-failed # (inactive queue)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    sqsMock
      .on('listQueues')
      .resolvesOnce({ QueueUrls: [
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}`,
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}_failed`
      ] })
    })
    sqsMock
      .on(GetQueueAttributesCommand)
      .resolvesOnce({
        Attributes: {
          ApproximateNumberOfMessages: '0',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    })
    sqsMock
      .on('getMetricStatistics')
      // Always return 0s
      .resolvesOnce({
        Label: params.MetricName,
        Datapoints: [
          { Timestamp: new Date(), Sum: 0, Metric: 'Count' },
          { Timestamp: new Date(), Sum: 0, Metric: 'Count' }
        ]
      })
    })
  })
  test('should print queue and fail queue name to stdout and exit 0', async () => {
    await expect(
      run(['idle-queues', 'test*', '--unpair', '--include-failed'])
    ).resolves.toEqual()
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Queue test has been idle for the last 60 minutes.')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Queue test_failed has been idle for the last 60 minutes.')
    const stdout = spy.log.mock.calls.join()
    expect(stdout).toContain('test\n')
    const stdout = spy.log.mock.calls.join()
    expect(stdout).toContain('test_failed\n')
    }))
})

describe('qdone idle-queues test # (no queues exist)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      const err = new Error('Queue does not exist.')
      err.code = 'AWS.SimpleQueueService.NonExistentQueue'
      callback(err)
    })
  })
  test('should print nothing to stdout and exit 0', async () => {
    await expect(
      run(['idle-queues', 'test'], null)
    ).resolves.toEqual()
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Queue does not exist.')
    }))
})

describe('qdone idle-queues \'test*\' # (main queue was recently deleted)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    sqsMock
      .on('listQueues')
      .resolvesOnce({ QueueUrls: [
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}`,
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}_failed`
      ] })
    })
    sqsMock
      .on(GetQueueAttributesCommand)
      const err = new Error('Queue does not exist.')
      err.code = 'AWS.SimpleQueueService.NonExistentQueue'
      callback(err)
    })
  })
  test('should print nothing to stdout and exit 0', async () => {
    await expect(
      run(['idle-queues', 'test*'], null)
    ).resolves.toEqual()
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Queue does not exist.')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('This error can occur when you run this command immediately after deleting a queue. Wait 60 seconds and try again.')
    }))
})

describe('qdone idle-queues --delete \'test*\' # (failed queue was recently deleted)', function () {
  beforeAll(function () {
    sqsMock
      .on(GetQueueUrlCommand)
      if (params.QueueName === 'qdone_test_failed') {
        const err = new Error('Queue does not exist.')
        err.code = 'AWS.SimpleQueueService.NonExistentQueue'
        callback(err, null)
      } else {
        .resolvesOnce({ QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
      }
    })
    sqsMock
      .on('listQueues')
      .resolvesOnce({ QueueUrls: [
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}`
        // `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}_failed`
      ] })
    })
    sqsMock
      .on('deleteQueue')
      if (params.QueueUrl === 'https://q.amazonaws.com/123456789101/qdone_test_failed') {
        const err = new Error('Queue does not exist.')
        err.code = 'AWS.SimpleQueueService.NonExistentQueue'
        callback(err)
        callback(err, null)
      } else {
        .resolvesOnce({})
      }
    })
    sqsMock
      .on(GetQueueAttributesCommand)
      if (params.QueueUrl === 'https://q.amazonaws.com/123456789101/qdone_test_failed') {
        const err = new Error('Queue does not exist.')
        err.code = 'AWS.SimpleQueueService.NonExistentQueue'
        callback(err, null)
      } else {
        .resolvesOnce({
          Attributes: {
            ApproximateNumberOfMessages: '0',
            ApproximateNumberOfMessagesDelayed: '0',
            ApproximateNumberOfMessagesNotVisible: '0'
          }
        })
      }
    })
    sqsMock
      .on('getMetricStatistics')
      // Always return 0s
      .resolvesOnce({
        Label: params.MetricName,
        Datapoints: [
          { Timestamp: new Date(), Sum: 0, Metric: 'Count' },
          { Timestamp: new Date(), Sum: 0, Metric: 'Count' }
        ]
      })
    })
  })
  test('should note the missing failed queue, print deleted queue to stdout and exit 0', async () => {
    await expect(
      run(['idle-queues', '--delete', 'test*'])
    ).resolves.toEqual()
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Queue test_failed does not exist.')
    const stderr = spy.error.mock.calls.join()
    expect(stderr).toContain('Deleted test')
    const stdout = spy.log.mock.calls.join()
    expect(stdout).toContain('test\n')
      expect(stdout).to.not.contain('test_failed')
    }))
})
*/
