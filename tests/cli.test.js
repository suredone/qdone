import { jest } from '@jest/globals'
import { readFileSync } from 'node:fs'
import { mockClient } from 'aws-sdk-client-mock'
import 'aws-sdk-client-mock-jest'

import {
  // ListQueuesCommand,
  GetQueueUrlCommand,
  SendMessageCommand
} from '@aws-sdk/client-sqs'

import { run } from '../src/cli.js'
import { qrlCacheClear } from '../src/qrlCache.js'
import { getSQSClient, setSQSClient } from '../src/sqs.js'

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

/*
describe('qdone enqueue --fifo testQueue true # (queue exists, fifo mode)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'sendMessage', function (params, callback) {
      callback(null, {
        MD5OfMessageAttributes: '00484c68...59e48f06',
        MD5OfMessageBody: '51b0a325...39163aa0',
        MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE'
      })
    })
  })
  test('should print id of enqueued message and exit 0',
    cliTest(['enqueue', '--fifo', 'testQueue', 'true'], function (result, stdout, stderr) {
      expect(stderr).to.contain('Enqueued job da68f62c-0c07-4bee-bf5f-7e856EXAMPLE')
    }))
})

describe('qdone enqueue --fifo --group-id gidtest testQueue true # (queue exists, fifo mode, explicit group)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'sendMessage', function (params, callback) {
      callback(null, {
        MD5OfMessageAttributes: '00484c68...59e48f06',
        MD5OfMessageBody: '51b0a325...39163aa0',
        MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE'
      })
    })
  })
  test('should print id of enqueued message and exit 0',
    cliTest(['enqueue', '--verbose', '--fifo', '--group-id', 'gidtest', 'testQueue', 'true'], function (result, stdout, stderr) {
      expect(stderr).to.contain('Enqueued job da68f62c-0c07-4bee-bf5f-7e856EXAMPLE')
    }))
})

describe('qdone enqueue --quiet testQueue true # (queue exists)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'sendMessage', function (params, callback) {
      callback(null, {
        MD5OfMessageAttributes: '00484c68...59e48f06',
        MD5OfMessageBody: '51b0a325...39163aa0',
        MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE'
      })
    })
  })
  test('should have no output and exit 0',
    cliTest(['enqueue', '--quiet', 'testQueue', 'true'], function (result, stdout, stderr) {
      expect(stderr).to.equal('')
      expect(stdout).to.equal('')
    }))
})

describe('qdone enqueue testQueue true # (queue does not exist)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      const err = new Error('Queue does not exist.')
      err.code = 'AWS.SimpleQueueService.NonExistentQueue'
      callback(err)
    })
    AWS.mock('SQS', 'createQueue', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'getQueueAttributes', function (params, callback) {
      callback(null, {
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
    AWS.mock('SQS', 'sendMessage', function (params, callback) {
      callback(null, {
        MD5OfMessageAttributes: '00484c68...59e48f06',
        MD5OfMessageBody: '51b0a325...39163aa0',
        MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE'
      })
    })
  })
  test('should create queues, print the id of enqueued message and exit 0',
    cliTest(['enqueue', 'testQueue', 'true'], function (result, stdout, stderr) {
      expect(stderr).to.contain('Creating fail queue testQueue_failed')
      expect(stderr).to.contain('Creating queue testQueue')
      expect(stderr).to.contain('Enqueued job da68f62c-0c07-4bee-bf5f-7e856EXAMPLE')
    }))
})

describe('qdone enqueue --quiet testQueue true # (queue does not exist)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      const err = new Error('Queue does not exist.')
      err.code = 'AWS.SimpleQueueService.NonExistentQueue'
      callback(err)
    })
    AWS.mock('SQS', 'createQueue', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'getQueueAttributes', function (params, callback) {
      callback(null, {
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
    AWS.mock('SQS', 'sendMessage', function (params, callback) {
      callback(null, {
        MD5OfMessageAttributes: '00484c68...59e48f06',
        MD5OfMessageBody: '51b0a325...39163aa0',
        MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE'
      })
    })
  })
  test('should create queues, print nothing and exit 0',
    cliTest(['enqueue', '--quiet', 'testQueue', 'true'], function (result, stdout, stderr) {
      expect(stderr).to.equal('')
      expect(stdout).to.equal('')
    }))
})

describe('qdone enqueue testQueue true # (unhandled error on fail queue creation)', function () {
  beforeAll(function () {
    var code = 'AWS.SimpleQueueService.NonExistentQueue'
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      const err = new Error('Queue does not exist.')
      err.code = code
      code = 'AWS.SimpleQueueService.SomeOtherError'
      callback(err)
    })
    AWS.mock('SQS', 'createQueue', function (params, callback) {
      // callback(null, {QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}`})
      const err = new Error('Some Other Error.')
      err.code = 'AWS.SimpleQueueService.SomeOtherError'
      callback(err)
    })
    AWS.mock('SQS', 'getQueueAttributes', function (params, callback) {
      callback(null, {
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
    AWS.mock('SQS', 'sendMessage', function (params, callback) {
      callback(null, {
        MD5OfMessageAttributes: '00484c68...59e48f06',
        MD5OfMessageBody: '51b0a325...39163aa0',
        MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE'
      })
    })
  })
  test('should print traceback and exit 1 with error',
    cliTest(['enqueue', '--verbose', 'testQueue', 'true'], null, function (err, stdout, stderr) {
      expect(err).to.be.an('error')
    }))
})

// Enqueue batch
describe('qdone enqueue-batch', function () {
  test('should print usage and exit 1 with error',
    cliTest(['enqueue-batch'], null, function (err, stdout, stderr) {
      expect(stdout).to.contain('usage: ')
      expect(stderr).to.contain('<file>')
      expect(err).to.be.an('error')
    }))
})

describe('qdone enqueue-batch --help', function () {
  test('should print usage and exit 0',
    cliTest(['enqueue-batch', '--help'], function (result, stdout, stderr) {
      expect(stdout).to.contain('usage: ')
      expect(stdout).to.contain('enqueue-batch')
    }))
})

describe('qdone enqueue-batch some_non_existent_file', function () {
  test('should exit 1 with error',
    cliTest(['enqueue-batch', 'some_non_existent_file'], null, function (err, stdout, stderr) {
      expect(stderr).to.contain('no such file or directory')
      expect(err).to.be.an('error')
    }))
})

describe('qdone enqueue-batch test/fixtures/test-unique01-x24.batch # (with no credentials)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      const err = new Error('Access to the resource https://sqs.us-east-1.amazonaws.com/ is denied.')
      err.code = 'AccessDenied'
      callback(err)
    })
  })
  test('should print usage and exit 1 with error',
    cliTest(['enqueue-batch', 'test/fixtures/test-unique01-x24.batch'], null, function (err, stdout, stderr) {
      expect(stdout).to.contain('You must provide')
      expect(stderr).to.contain('Access to the resource https://sqs.us-east-1.amazonaws.com/ is denied.')
      expect(err).to.be.an('error')
    }))
})

describe('qdone enqueue-batch test/fixtures/test-unique01-x24.batch # (queue exists)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    var messageId = 0
    AWS.mock('SQS', 'sendMessageBatch', function (params, callback) {
      callback(null, {
        Failed: [],
        Successful: params.Entries.map(message => ({
          MD5OfMessageAttributes: '00484c68...59e48f06',
          MD5OfMessageBody: '51b0a325...39163aa0',
          MessageId: 'da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId++
        }))
      })
    })
  })
  test('should print id of enqueued messages, use 3 requests, print total count and exit 0',
    cliTest(['enqueue-batch', 'test/fixtures/test-unique01-x24.batch'], function (result, stdout, stderr) {
      for (var messageId = 0; messageId < 24; messageId++) {
        expect(stderr).to.contain('Enqueued job da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId)
      }
      expect(stderr).to.contain('Enqueued 24 jobs')
      expect(stderr).to.contain('request 1')
      expect(stderr).to.contain('request 2')
      expect(stderr).to.contain('request 3')
    }))
})

describe('qdone enqueue-batch --fifo --group-id-per-message test/fixtures/test-unique01-x24.batch # (queue exists, group ids should be unique)', function () {
  let groupIds
  beforeAll(function () {
    groupIds = {}
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    var messageId = 0
    AWS.mock('SQS', 'sendMessageBatch', function (params, callback) {
      params.Entries.forEach(message => {
        groupIds[message.MessageGroupId] = true
      })
      callback(null, {
        Failed: [],
        Successful: params.Entries.map(message => ({
          MD5OfMessageAttributes: '00484c68...59e48f06',
          MD5OfMessageBody: '51b0a325...39163aa0',
          MessageId: 'da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId++
        }))
      })
    })
  })
  test('should print id of enqueued messages, use 3 requests, use unique group ids for every message, print total count and exit 0',
    cliTest(['enqueue-batch', '--fifo', '--group-id-per-message', 'test/fixtures/test-unique01-x24.batch'], function (result, stdout, stderr) {
      for (var messageId = 0; messageId < 24; messageId++) {
        expect(stderr).to.contain('Enqueued job da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId)
      }
      expect(Object.keys(groupIds).length).to.equal(24)
      expect(stderr).to.contain('Enqueued 24 jobs')
      expect(stderr).to.contain('request 1')
      expect(stderr).to.contain('request 2')
      expect(stderr).to.contain('request 3')
    }))
})

describe('qdone enqueue-batch test/fixtures/test-unique01-x24.batch # (queue exists, some failures)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    var messageId = 0
    AWS.mock('SQS', 'sendMessageBatch', function (params, callback) {
      callback(null, {
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
  test('should exit 1 and show which messages failed',
    cliTest(['enqueue-batch', '--verbose', 'test/fixtures/test-unique01-x24.batch'], null, function (err, stdout, stderr) {
      expect(stderr).to.contain('Error: One or more message failures')
      expect(err).to.be.an('error')
      // Expect some ids of failed messages
      for (var messageId = 0; messageId < 2; messageId++) {
        expect(stderr).to.contain('da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId)
      }
    }))
})

describe('qdone enqueue-batch --quiet test/fixtures/test-unique01-x24.batch # (queue exists)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    var messageId = 0
    AWS.mock('SQS', 'sendMessageBatch', function (params, callback) {
      callback(null, {
        Failed: [],
        Successful: params.Entries.map(message => ({
          MD5OfMessageAttributes: '00484c68...59e48f06',
          MD5OfMessageBody: '51b0a325...39163aa0',
          MessageId: 'da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId++
        }))
      })
    })
  })
  test('should have no output and exit 0',
    cliTest(['enqueue-batch', '--quiet', 'test/fixtures/test-unique01-x24.batch'], function (result, stdout, stderr) {
      expect(stderr).to.equal('')
      expect(stdout).to.equal('')
    }))
})

describe('qdone enqueue-batch --fifo test/fixtures/test-fifo01-x24.batch # (queue does not exist)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      const err = new Error('Queue does not exist.')
      err.code = 'AWS.SimpleQueueService.NonExistentQueue'
      callback(err)
    })
    AWS.mock('SQS', 'createQueue', function (params, callback) {
      expect(params.QueueName.slice(-'.fifo'.length) === '.fifo')
      expect(params.FifoQueue === 'true')
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'getQueueAttributes', function (params, callback) {
      callback(null, {
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
    AWS.mock('SQS', 'sendMessageBatch', function (params, callback) {
      callback(null, {
        Failed: [],
        Successful: params.Entries.map(message => ({
          MD5OfMessageAttributes: '00484c68...59e48f06',
          MD5OfMessageBody: '51b0a325...39163aa0',
          MessageId: 'da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId++
        }))
      })
    })
  })
  test('should print id of enqueued messages, use 3 requests, print total count and exit 0',
    cliTest(['enqueue-batch', '--verbose', '--fifo', 'test/fixtures/test-fifo01-x24.batch'], function (result, stdout, stderr) {
      for (var messageId = 0; messageId < 24; messageId++) {
        expect(stderr).to.contain('Enqueued job da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId)
      }
      expect(stderr).to.contain('Enqueued 24 jobs')
      expect(stderr).to.contain('request 1')
      expect(stderr).to.contain('request 2')
      expect(stderr).to.contain('request 3')
      expect(stderr).to.not.contain('request 4')
    }))
})

describe('qdone enqueue-batch test/fixtures/test-unique01-x24.batch # (queue does not exist)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      const err = new Error('Queue does not exist.')
      err.code = 'AWS.SimpleQueueService.NonExistentQueue'
      callback(err)
    })
    AWS.mock('SQS', 'createQueue', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'getQueueAttributes', function (params, callback) {
      callback(null, {
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
    AWS.mock('SQS', 'sendMessageBatch', function (params, callback) {
      callback(null, {
        Failed: [],
        Successful: params.Entries.map(message => ({
          MD5OfMessageAttributes: '00484c68...59e48f06',
          MD5OfMessageBody: '51b0a325...39163aa0',
          MessageId: 'da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId++
        }))
      })
    })
  })
  test('should create queues, print ids of enqueued messages, use 3 requests, print total count and exit 0',
    cliTest(['enqueue-batch', 'test/fixtures/test-unique01-x24.batch'], function (result, stdout, stderr) {
      expect(stderr).to.contain('Creating fail queue test_failed')
      expect(stderr).to.contain('Creating queue test')
      for (var messageId = 0; messageId < 24; messageId++) {
        expect(stderr).to.contain('Enqueued job da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId)
      }
      expect(stderr).to.contain('Enqueued 24 jobs')
      expect(stderr).to.contain('request 1')
      expect(stderr).to.contain('request 2')
      expect(stderr).to.contain('request 3')
    }))
})

describe('qdone enqueue-batch test/fixtures/test-unique{01-x24.batch,02-x24.batch,24-x24.batch,24-x240.batch} # (ensemble fixtures, queue exists)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    var messageId = 0
    AWS.mock('SQS', 'sendMessageBatch', function (params, callback) {
      callback(null, {
        Failed: [],
        Successful: params.Entries.map(message => ({
          MD5OfMessageAttributes: '00484c68...59e48f06',
          MD5OfMessageBody: '51b0a325...39163aa0',
          MessageId: 'da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId++
        }))
      })
    })
  })
  test('should print id of enqueued messages, use 53 requests, print total count and exit 0',
    cliTest([
      'enqueue-batch',
      'test/fixtures/test-unique01-x24.batch',
      'test/fixtures/test-unique02-x24.batch',
      'test/fixtures/test-unique24-x24.batch',
      'test/fixtures/test-unique24-x240.batch'
    ], function (result, stdout, stderr) {
      for (var messageId = 0; messageId < 312; messageId++) {
        expect(stderr).to.contain('Enqueued job da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId)
      }
      expect(stderr).to.contain('Enqueued 312 jobs')
      expect(stderr).to.contain('request 1')
      expect(stderr).to.contain('request 2')
      expect(stderr).to.contain('request 53')
    }))
})

describe('qdone enqueue-batch test/fixtures/test-too-big-1.batch # (messages too big for full batch)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      const err = new Error('Queue does not exist.')
      err.code = 'AWS.SimpleQueueService.NonExistentQueue'
      callback(err)
    })
    AWS.mock('SQS', 'createQueue', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'getQueueAttributes', function (params, callback) {
      callback(null, {
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
    AWS.mock('SQS', 'sendMessageBatch', function (params, callback) {
      callback(null, {
        Failed: [],
        Successful: params.Entries.map(message => ({
          MD5OfMessageAttributes: '00484c68...59e48f06',
          MD5OfMessageBody: '51b0a325...39163aa0',
          MessageId: 'da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId++
        }))
      })
    })
  })
  test('should print ids of enqueued messages, use 5 requests, print total count and exit 0',
    cliTest(['enqueue-batch', 'test/fixtures/test-too-big-1.batch'], function (result, stdout, stderr) {
      for (var messageId = 0; messageId < 10; messageId++) {
        expect(stderr).to.contain('Enqueued job da68f62c-0c07-4bee-bf5f-56EXAMPLE-' + messageId)
      }
      expect(stderr).to.contain('Enqueued 10 jobs')
      expect(stderr).to.contain('request 1')
      expect(stderr).to.contain('request 2')
      expect(stderr).to.contain('request 3')
      expect(stderr).to.contain('request 4')
      expect(stderr).to.contain('request 5')
    }))
})

// Worker
describe('qdone worker', function () {
  test('should print usage and exit 1 with error',
    cliTest(['worker'], null, function (err, stdout, stderr) {
      expect(stdout).to.contain('usage: ')
      expect(stderr).to.contain('<queue>')
      expect(err).to.be.an('error')
    }))
})

describe('qdone worker --help', function () {
  test('should print usage and exit 0',
    cliTest(['worker', '--help'], function (result, stdout, stderr) {
      expect(stdout).to.contain('usage: ')
      expect(stdout).to.contain('worker')
    }))
})

describe('qdone worker some_non_existent_queue --drain', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      const err = new Error('Queue does not exist.')
      err.code = 'AWS.SimpleQueueService.NonExistentQueue'
      callback(err)
    })
  })
  test('should complain and exit 0',
    cliTest(['worker', 'some_non_existent_queue', '--drain'], null, function (result, stdout, stderr) {
      expect(stderr).to.contain('AWS.SimpleQueueService.NonExistentQueue')
    }))
})

describe('qdone worker test --drain # (no jobs)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'listQueues', function (params, callback) {
      callback(null, { QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    AWS.mock('SQS', 'receiveMessage', function (params, callback) {
      callback(null, {})
    })
    AWS.mock('SQS', 'deleteMessage', function (params, callback) {
      callback(null, {})
    })
  })
  test('should execute the job successfully and exit 0',
    cliTest(['worker', 'test', '--drain'], function (result, stdout, stderr) {
      expect(stderr).to.contain('Looking for work on test')
      expect(stderr).to.contain('Ran 0 jobs: 0 succeeded 0 failed')
    }))
})

describe('qdone worker test --drain # (1 successful job)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'listQueues', function (params, callback) {
      callback(null, { QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    AWS.mock('SQS', 'receiveMessage', function (params, callback) {
      callback(null, { Messages: [
        { MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE', Body: 'true', ReceiptHandle: 'AQEBzbVv...fqNzFw==' }
      ] })
      AWS.restore('SQS', 'receiveMessage')
      // Subsequent calls return no message
      AWS.mock('SQS', 'receiveMessage', function (params, callback) {
        callback(null, {})
      })
    })
    AWS.mock('SQS', 'deleteMessage', function (params, callback) {
      callback(null, {})
    })
  })
  test('should execute the job successfully and exit 0',
    cliTest(['worker', 'test', '--drain'], function (result, stdout, stderr) {
      expect(stderr).to.contain('Looking for work on test')
      expect(stderr).to.contain('Found job da68f62c-0c07-4bee-bf5f-7e856EXAMPLE')
      expect(stderr).to.contain('SUCCESS')
      expect(stderr).to.contain('Ran 1 jobs: 1 succeeded 0 failed')
    }))
})

describe('qdone worker test --drain --quiet # (1 failed job)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'listQueues', function (params, callback) {
      callback(null, { QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    AWS.mock('SQS', 'receiveMessage', function (params, callback) {
      callback(null, { Messages: [
        { MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE', Body: 'false', ReceiptHandle: 'AQEBzbVv...fqNzFw==' }
      ] })
      AWS.restore('SQS', 'receiveMessage')
      // Subsequent calls return no message
      AWS.mock('SQS', 'receiveMessage', function (params, callback) {
        callback(null, {})
      })
    })
    AWS.mock('SQS', 'deleteMessage', function (params, callback) {
      callback(null, {})
    })
  })
  test('should execute the job successfully and exit 0',
    cliTest(['worker', 'test', '--drain', '--quiet'], function (result, stdout, stderr) {
      expect(stdout).to.contain('"event":"JOB_FAILED"')
      expect(stdout).to.contain('"command":"false"')
      expect(stdout).to.contain('"timestamp"')
      expect(stdout).to.contain('"job"')
      expect(stdout).to.contain('"exitCode"')
    }))
})

describe('qdone worker test --drain --kill-after 1 --wait-time 1 --quiet # (job runs past kill timer)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'listQueues', function (params, callback) {
      callback(null, { QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    AWS.mock('SQS', 'receiveMessage', function (params, callback) {
      callback(null, { Messages: [
        { MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE', Body: 'bash test/fixtures/test-child-kill-linux.sh', ReceiptHandle: 'AQEBzbVv...fqNzFw==' }
      ] })
      AWS.restore('SQS', 'receiveMessage')
      // Subsequent calls return no message
      AWS.mock('SQS', 'receiveMessage', function (params, callback) {
        callback(null, {})
      })
      process.nextTick(function () {
        clock.tick(1500)
      })
    })
    AWS.mock('SQS', 'deleteMessage', function (params, callback) {
      callback(null, {})
    })
  })
  test('should execute the job successfully and exit 0',
    cliTest(['worker', 'test', '--drain', '--kill-after', '1', '--wait-time', '1'], function (result, stdout, stderr) {
      expect(stderr).to.contain('FAILED')
      // Check that file does not continue to be written to
      expect(fs.readFileSync('/tmp/qdone-test-child-kill-linux.out').toString()).to.equal('terminated\n')
    }))
})

describe('qdone worker "test*" --drain # (9 queues, 1 successful job per queue)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'listQueues', function (params, callback) {
      callback(null, { QueueUrls: [
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
    AWS.mock('SQS', 'receiveMessage', function (params, callback) {
      callback(null, { Messages: [
        { MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE-' + params.QueueUrl.slice(-1), Body: 'true', ReceiptHandle: 'AQEBzbVv...fqNzFw==' }
      ] })
      if (params.QueueUrl === params.QueueUrl.slice(0, -1) + '9') {
        AWS.restore('SQS', 'receiveMessage')
        // Subsequent calls return no message
        AWS.mock('SQS', 'receiveMessage', function (params, callback) {
          callback(null, {})
        })
      }
    })
    AWS.mock('SQS', 'deleteMessage', function (params, callback) {
      callback(null, {})
    })
  })
  test('should execute the job successfully and exit 0',
    cliTest(['worker', 'test*', '--drain'], function (result, stdout, stderr) {
      [1, 2, 3, 4, 5, 5, 6, 7, 8, 9].forEach(index => {
        expect(stderr).to.contain('Looking for work on test' + index)
        expect(stderr).to.contain('Found job da68f62c-0c07-4bee-bf5f-7e856EXAMPLE-' + index)
      })
      expect(stderr).to.contain('Ran 9 jobs: 9 succeeded 0 failed')
    }))
})

describe('qdone worker test --drain # (1 successful job, time extended)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'listQueues', function (params, callback) {
      callback(null, { QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    AWS.mock('SQS', 'changeMessageVisibility', function (params, callback) {
      callback(null, {})
    })
    AWS.mock('SQS', 'receiveMessage', function (params, callback) {
      callback(null, { Messages: [
        { MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE', Body: 'sleep 1', ReceiptHandle: 'AQEBzbVv...fqNzFw==' }
      ] })
      AWS.restore('SQS', 'receiveMessage')
      // Subsequent calls return no message
      AWS.mock('SQS', 'receiveMessage', function (params, callback) {
        callback(null, {})
      })
      process.nextTick(function () {
        clock.tick(15000)
      })
    })
    AWS.mock('SQS', 'deleteMessage', function (params, callback) {
      callback(null, {})
    })
  })
  test('should execute the job successfully and exit 0',
    cliTest(['worker', 'test', '--drain'], function (result, stdout, stderr) {
      expect(stderr).to.contain('Looking for work on test')
      expect(stderr).to.contain('Found job da68f62c-0c07-4bee-bf5f-7e856EXAMPLE')
      expect(stderr).to.contain('seconds, requesting another')
      expect(stderr).to.contain('SUCCESS')
      expect(stderr).to.contain('Ran 1 jobs: 1 succeeded 0 failed')
    }))
})

describe('qdone worker test --drain --quiet # (1 successful job, time extended)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'listQueues', function (params, callback) {
      callback(null, { QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    AWS.mock('SQS', 'changeMessageVisibility', function (params, callback) {
      callback(null, {})
    })
    AWS.mock('SQS', 'receiveMessage', function (params, callback) {
      callback(null, { Messages: [
        { MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE', Body: 'sleep 1', ReceiptHandle: 'AQEBzbVv...fqNzFw==' }
      ] })
      AWS.restore('SQS', 'receiveMessage')
      // Subsequent calls return no message
      AWS.mock('SQS', 'receiveMessage', function (params, callback) {
        callback(null, {})
      })
      process.nextTick(function () {
        clock.tick(15000)
      })
    })
    AWS.mock('SQS', 'deleteMessage', function (params, callback) {
      callback(null, {})
    })
  })
  test('should have no output and exit 0',
    cliTest(['worker', 'test', '--drain', '--quiet'], function (result, stdout, stderr) {
      expect(stderr).to.equal('')
      expect(stdout).to.equal('')
    }))
})

describe('qdone worker "test*" --drain --active-only # (4 queues, 2 full, 2 empty)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'listQueues', function (params, callback) {
      callback(null, { QueueUrls: [
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}1`,
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}2`,
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}3`,
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}4`
      ] })
    })
    AWS.mock('SQS', 'getQueueAttributes', function (params, callback) {
      const lastLetter = params.QueueUrl.slice(-1)
      callback(null, {
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
    AWS.mock('SQS', 'receiveMessage', function (params, callback) {
      callback(null, { Messages: [
        { MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE-' + params.QueueUrl.slice(-1), Body: 'true', ReceiptHandle: 'AQEBzbVv...fqNzFw==' }
      ] })
      if (params.QueueUrl === params.QueueUrl.slice(0, -1) + '2') {
        AWS.restore('SQS', 'receiveMessage')
        // Subsequent calls return no message
        AWS.mock('SQS', 'receiveMessage', function (params, callback) {
          callback(null, {})
        })
      }
    })
    AWS.mock('SQS', 'deleteMessage', function (params, callback) {
      callback(null, {})
    })
  })
  test('should execute the job successfully and exit 0',
    cliTest(['worker', 'test*', '--drain', '--active-only'], function (result, stdout, stderr) {
      expect(stderr).to.contain('Found job da68f62c-0c07-4bee-bf5f-7e856EXAMPLE-1')
      expect(stderr).to.contain('Found job da68f62c-0c07-4bee-bf5f-7e856EXAMPLE-2')
      expect(stderr).to.not.contain('Found job da68f62c-0c07-4bee-bf5f-7e856EXAMPLE-3')
      expect(stderr).to.not.contain('Found job da68f62c-0c07-4bee-bf5f-7e856EXAMPLE-4')
      expect(stderr).to.contain('Ran 2 jobs: 2 succeeded 0 failed')
    }))
})

describe('qdone worker test # (1 successful job + SIGTERM)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'listQueues', function (params, callback) {
      callback(null, { QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    AWS.mock('SQS', 'receiveMessage', function (params, callback) {
      callback(null, { Messages: [
        { MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE', Body: 'sleep 1', ReceiptHandle: 'AQEBzbVv...fqNzFw==' }
      ] })
      AWS.restore('SQS', 'receiveMessage')
      // Subsequent calls return no message
      AWS.mock('SQS', 'receiveMessage', function (params, callback) {
        callback(null, {})
      })
      // And here we trigger a SIGTERM
      process.kill(process.pid, 'SIGTERM')
    })
    AWS.mock('SQS', 'deleteMessage', function (params, callback) {
      callback(null, {})
    })
  })
  test('should begin executing the job, acknowledge a SIGTERM and successfully and exit 0 after the job completes',
    cliTest(['worker', 'test'], function (result, stdout, stderr) {
      expect(stderr).to.contain('Looking for work on test')
      expect(stderr).to.contain('Found job da68f62c-0c07-4bee-bf5f-7e856EXAMPLE')
      expect(stderr).to.contain('Shutdown requested')
      expect(stderr).to.contain('SUCCESS')
    }))
})

// Idle queues

describe('qdone idle-queues', function () {
  test('should print usage and exit 1 with error',
    cliTest(['idle-queues'], null, function (err, stdout, stderr) {
      expect(stdout).to.contain('usage: ')
      expect(stderr).to.contain('<queue>')
      expect(err).to.be.an('error')
    }))
})

describe('qdone idle-queues --help', function () {
  test('should print usage and exit 0',
    cliTest(['idle-queues', '--help'], function (result, stdout, stderr) {
      expect(stdout).to.contain('usage: ')
      expect(stdout).to.contain('idle-queues')
    }))
})

describe('qdone idle-queues test --include-failed', function () {
  test('should print usage and note about --include-failed and exit 1 with error',
    cliTest(['idle-queues', 'test', '--include-failed'], null, function (err, stdout, stderr) {
      expect(stdout).to.contain('usage: ')
      expect(stderr).to.contain('--include-failed should be used with --unpair')
      expect(err).to.be.an('error')
    }))
})

describe('qdone idle-queues test # (active queue shortcut via SQS API)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'listQueues', function (params, callback) {
      callback(null, { QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    AWS.mock('SQS', 'getQueueAttributes', function (params, callback) {
      callback(null, {
        Attributes: {
          ApproximateNumberOfMessages: '1',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    })
  })
  test('should make no CloudWatch calls, print nothing to stdout and exit 0',
    cliTest(['idle-queues', 'test'], function (result, stdout, stderr) {
      expect(stderr).to.contain('Queue test has been active in the last 60 minutes.')
      expect(stdout).to.equal('')
    }))
})

describe('qdone idle-queues test --unpair # (active queue shortcut via SQS API)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'listQueues', function (params, callback) {
      callback(null, { QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    AWS.mock('SQS', 'getQueueAttributes', function (params, callback) {
      callback(null, {
        Attributes: {
          ApproximateNumberOfMessages: '1',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    })
  })
  test('should make no CloudWatch calls, print nothing to stdout and exit 0',
    cliTest(['idle-queues', 'test', '--unpair'], function (result, stdout, stderr) {
      expect(stderr).to.contain('Queue test has been active in the last 60 minutes.')
      expect(stdout).to.equal('')
    }))
})

describe('qdone idle-queues test --cache-uri redis://localhost # (cached getQueueAttributes call)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'listQueues', function (params, callback) {
      callback(null, { QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    AWS.mock('SQS', 'getQueueAttributes', function (params, callback) {
      callback(null, {
        Attributes: {
          ApproximateNumberOfMessages: '1',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    })
  })
  test('should make one call to getQueueAttributes, print nothing to stdout and exit 0',
    cliTest(['idle-queues', 'test', '--cache-uri', 'redis://localhost'], function (result, stdout, stderr) {
      expect(stderr).to.contain('Queue test has been active in the last 60 minutes.')
      expect(stdout).to.equal('')
    }))
})

describe('qdone idle-queues test # (active queue, multiple CloudWatch calls)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'listQueues', function (params, callback) {
      callback(null, { QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    AWS.mock('SQS', 'getQueueAttributes', function (params, callback) {
      callback(null, {
        Attributes: {
          ApproximateNumberOfMessages: '0',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    })
    AWS.mock('CloudWatch', 'getMetricStatistics', function (params, callback) {
      if (params.MetricName === 'ApproximateNumberOfMessagesDelayed') {
        callback(null, {
          Label: params.MetricName,
          Datapoints: [
            { Timestamp: new Date(), Sum: 0, Metric: 'Count' },
            { Timestamp: new Date(), Sum: 1, Metric: 'Count' }
          ]
        })
      } else {
        callback(null, {
          Label: params.MetricName,
          Datapoints: [
            { Timestamp: new Date(), Sum: 0, Metric: 'Count' },
            { Timestamp: new Date(), Sum: 0, Metric: 'Count' }
          ]
        })
      }
    })
  })
  test('should make CloudWatch calls, print nothing to stdout and exit 0',
    cliTest(['idle-queues', 'test'], function (result, stdout, stderr) {
      expect(stderr).to.contain('Queue test has been active in the last 60 minutes.')
      expect(stdout).to.equal('')
    }))
})

describe('qdone idle-queues test # (inactive queue)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'listQueues', function (params, callback) {
      callback(null, { QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    AWS.mock('SQS', 'getQueueAttributes', function (params, callback) {
      callback(null, {
        Attributes: {
          ApproximateNumberOfMessages: '0',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    })
    AWS.mock('CloudWatch', 'getMetricStatistics', function (params, callback) {
      // Always return 0s
      callback(null, {
        Label: params.MetricName,
        Datapoints: [
          { Timestamp: new Date(), Sum: 0, Metric: 'Count' },
          { Timestamp: new Date(), Sum: 0, Metric: 'Count' }
        ]
      })
    })
  })
  test('should print queue name to stdout and exit 0',
    cliTest(['idle-queues', 'test'], function (result, stdout, stderr) {
      expect(stderr).to.contain('Queue test has been idle for the last 60 minutes.')
      expect(stdout).to.contain('test\n')
    }))
})

describe('qdone idle-queues test --delete # (inactive queue)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'listQueues', function (params, callback) {
      callback(null, { QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    AWS.mock('SQS', 'getQueueAttributes', function (params, callback) {
      callback(null, {
        Attributes: {
          ApproximateNumberOfMessages: '0',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    })
    AWS.mock('CloudWatch', 'getMetricStatistics', function (params, callback) {
      // Always return 0s
      callback(null, {
        Label: params.MetricName,
        Datapoints: [
          { Timestamp: new Date(), Sum: 0, Metric: 'Count' },
          { Timestamp: new Date(), Sum: 0, Metric: 'Count' }
        ]
      })
    })
    AWS.mock('SQS', 'deleteQueue', function (params, callback) {
      callback(null, {})
    })
  })
  test('should print queue name to stdout and exit 0',
    cliTest(['idle-queues', 'test', '--delete'], function (result, stdout, stderr) {
      expect(stderr).to.contain('Queue test has been idle for the last 60 minutes.')
      expect(stderr).to.contain('Deleted test')
      expect(stdout).to.contain('test\n')
    }))
})

describe('qdone idle-queues test --unpair # (inactive queue)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'listQueues', function (params, callback) {
      callback(null, { QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    AWS.mock('SQS', 'getQueueAttributes', function (params, callback) {
      callback(null, {
        Attributes: {
          ApproximateNumberOfMessages: '0',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    })
    AWS.mock('CloudWatch', 'getMetricStatistics', function (params, callback) {
      // Always return 0s
      callback(null, {
        Label: params.MetricName,
        Datapoints: [
          { Timestamp: new Date(), Sum: 0, Metric: 'Count' },
          { Timestamp: new Date(), Sum: 0, Metric: 'Count' }
        ]
      })
    })
  })
  test('should print queue name to stdout and exit 0',
    cliTest(['idle-queues', 'test', '--unpair'], function (result, stdout, stderr) {
      expect(stderr).to.contain('Queue test has been idle for the last 60 minutes.')
      expect(stdout).to.contain('test\n')
    }))
})

describe('qdone idle-queues test --unpair --delete # (inactive queue)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'listQueues', function (params, callback) {
      callback(null, { QueueUrls: [`https://q.amazonaws.com/123456789101/${params.QueueName}`] })
    })
    AWS.mock('SQS', 'getQueueAttributes', function (params, callback) {
      callback(null, {
        Attributes: {
          ApproximateNumberOfMessages: '0',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    })
    AWS.mock('CloudWatch', 'getMetricStatistics', function (params, callback) {
      // Always return 0s
      callback(null, {
        Label: params.MetricName,
        Datapoints: [
          { Timestamp: new Date(), Sum: 0, Metric: 'Count' },
          { Timestamp: new Date(), Sum: 0, Metric: 'Count' }
        ]
      })
    })
    AWS.mock('SQS', 'deleteQueue', function (params, callback) {
      callback(null, {})
    })
  })
  test('should print queue name to stdout and exit 0',
    cliTest(['idle-queues', 'test', '--unpair', '--delete'], function (result, stdout, stderr) {
      expect(stderr).to.contain('Queue test has been idle for the last 60 minutes.')
      expect(stderr).to.contain('Deleted test')
      expect(stdout).to.contain('test\n')
    }))
})

describe('qdone idle-queues test # (primary queue is idle, failed queue is active)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'listQueues', function (params, callback) {
      callback(null, { QueueUrls: [
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}`,
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}_failed`
      ] })
    })
    AWS.mock('SQS', 'getQueueAttributes', function (params, callback) {
      callback(null, {
        Attributes: {
          ApproximateNumberOfMessages: '0',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    })
    AWS.mock('CloudWatch', 'getMetricStatistics', function (params, callback) {
      // Always return data for failed queue
      if (params.Dimensions[0].Value === 'qdone_test_failed') {
        callback(null, {
          Label: params.MetricName,
          Datapoints: [
            { Timestamp: new Date(), Sum: 0, Metric: 'Count' },
            { Timestamp: new Date(), Sum: 1, Metric: 'Count' }
          ]
        })
      } else {
        callback(null, {
          Label: params.MetricName,
          Datapoints: [
            { Timestamp: new Date(), Sum: 0, Metric: 'Count' },
            { Timestamp: new Date(), Sum: 0, Metric: 'Count' }
          ]
        })
      }
    })
  })
  test('should print queue name to stdout and exit 0',
    cliTest(['idle-queues', 'test'], function (result, stdout, stderr) {
      expect(stderr).to.contain('Queue test has been idle for the last 60 minutes.')
      expect(stderr).to.contain('Queue test_failed has been active in the last 60 minutes.')
    }))
})

describe('qdone idle-queues test.fifo # (primary queue is idle and a FIFO, failed queue is active)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'listQueues', function (params, callback) {
      callback(null, { QueueUrls: [
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}`,
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}_failed.fifo`
      ] })
    })
    AWS.mock('SQS', 'getQueueAttributes', function (params, callback) {
      callback(null, {
        Attributes: {
          ApproximateNumberOfMessages: '0',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    })
    AWS.mock('CloudWatch', 'getMetricStatistics', function (params, callback) {
      // Always return data for failed queue
      if (params.Dimensions[0].Value === 'qdone_test_failed.fifo') {
        callback(null, {
          Label: params.MetricName,
          Datapoints: [
            { Timestamp: new Date(), Sum: 0, Metric: 'Count' },
            { Timestamp: new Date(), Sum: 1, Metric: 'Count' }
          ]
        })
      } else {
        callback(null, {
          Label: params.MetricName,
          Datapoints: [
            { Timestamp: new Date(), Sum: 0, Metric: 'Count' },
            { Timestamp: new Date(), Sum: 0, Metric: 'Count' }
          ]
        })
      }
    })
  })
  test('should print queue name to stdout and exit 0',
    cliTest(['idle-queues', 'test.fifo'], function (result, stdout, stderr) {
      expect(stderr).to.contain('Queue test.fifo has been idle for the last 60 minutes.')
      expect(stderr).to.contain('Queue test_failed.fifo has been active in the last 60 minutes.')
    }))
})

describe('qdone idle-queues \'test*\' --unpair --include-failed # (inactive queue)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'listQueues', function (params, callback) {
      callback(null, { QueueUrls: [
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}`,
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}_failed`
      ] })
    })
    AWS.mock('SQS', 'getQueueAttributes', function (params, callback) {
      callback(null, {
        Attributes: {
          ApproximateNumberOfMessages: '0',
          ApproximateNumberOfMessagesDelayed: '0',
          ApproximateNumberOfMessagesNotVisible: '0'
        }
      })
    })
    AWS.mock('CloudWatch', 'getMetricStatistics', function (params, callback) {
      // Always return 0s
      callback(null, {
        Label: params.MetricName,
        Datapoints: [
          { Timestamp: new Date(), Sum: 0, Metric: 'Count' },
          { Timestamp: new Date(), Sum: 0, Metric: 'Count' }
        ]
      })
    })
  })
  test('should print queue and fail queue name to stdout and exit 0',
    cliTest(['idle-queues', 'test*', '--unpair', '--include-failed'], function (result, stdout, stderr) {
      expect(stderr).to.contain('Queue test has been idle for the last 60 minutes.')
      expect(stderr).to.contain('Queue test_failed has been idle for the last 60 minutes.')
      expect(stdout).to.contain('test\n')
      expect(stdout).to.contain('test_failed\n')
    }))
})

describe('qdone idle-queues test # (no queues exist)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      const err = new Error('Queue does not exist.')
      err.code = 'AWS.SimpleQueueService.NonExistentQueue'
      callback(err)
    })
  })
  test('should print nothing to stdout and exit 0',
    cliTest(['idle-queues', 'test'], null, function (result, stdout, stderr) {
      expect(stderr).to.contain('Queue does not exist.')
    }))
})

describe('qdone idle-queues \'test*\' # (main queue was recently deleted)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
    })
    AWS.mock('SQS', 'listQueues', function (params, callback) {
      callback(null, { QueueUrls: [
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}`,
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}_failed`
      ] })
    })
    AWS.mock('SQS', 'getQueueAttributes', function (params, callback) {
      const err = new Error('Queue does not exist.')
      err.code = 'AWS.SimpleQueueService.NonExistentQueue'
      callback(err)
    })
  })
  test('should print nothing to stdout and exit 0',
    cliTest(['idle-queues', 'test*'], null, function (result, stdout, stderr) {
      expect(stderr).to.contain('Queue does not exist.')
      expect(stderr).to.contain('This error can occur when you run this command immediately after deleting a queue. Wait 60 seconds and try again.')
    }))
})

describe('qdone idle-queues --delete \'test*\' # (failed queue was recently deleted)', function () {
  beforeAll(function () {
    AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
      if (params.QueueName === 'qdone_test_failed') {
        const err = new Error('Queue does not exist.')
        err.code = 'AWS.SimpleQueueService.NonExistentQueue'
        callback(err, null)
      } else {
        callback(null, { QueueUrl: `https://q.amazonaws.com/123456789101/${params.QueueName}` })
      }
    })
    AWS.mock('SQS', 'listQueues', function (params, callback) {
      callback(null, { QueueUrls: [
        `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}`
        // `https://q.amazonaws.com/123456789101/${params.QueueNamePrefix}_failed`
      ] })
    })
    AWS.mock('SQS', 'deleteQueue', function (params, callback) {
      if (params.QueueUrl === 'https://q.amazonaws.com/123456789101/qdone_test_failed') {
        const err = new Error('Queue does not exist.')
        err.code = 'AWS.SimpleQueueService.NonExistentQueue'
        callback(err)
        callback(err, null)
      } else {
        callback(null, {})
      }
    })
    AWS.mock('SQS', 'getQueueAttributes', function (params, callback) {
      if (params.QueueUrl === 'https://q.amazonaws.com/123456789101/qdone_test_failed') {
        const err = new Error('Queue does not exist.')
        err.code = 'AWS.SimpleQueueService.NonExistentQueue'
        callback(err, null)
      } else {
        callback(null, {
          Attributes: {
            ApproximateNumberOfMessages: '0',
            ApproximateNumberOfMessagesDelayed: '0',
            ApproximateNumberOfMessagesNotVisible: '0'
          }
        })
      }
    })
    AWS.mock('CloudWatch', 'getMetricStatistics', function (params, callback) {
      // Always return 0s
      callback(null, {
        Label: params.MetricName,
        Datapoints: [
          { Timestamp: new Date(), Sum: 0, Metric: 'Count' },
          { Timestamp: new Date(), Sum: 0, Metric: 'Count' }
        ]
      })
    })
  })
  test('should note the missing failed queue, print deleted queue to stdout and exit 0',
    cliTest(['idle-queues', '--delete', 'test*'], function (result, stdout, stderr) {
      expect(stderr).to.contain('Queue test_failed does not exist.')
      expect(stderr).to.contain('Deleted test')
      expect(stdout).to.contain('test\n')
      expect(stdout).to.not.contain('test_failed')
    }))
})
*/
