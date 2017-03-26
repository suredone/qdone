/* eslint-env mocha */

const chai = require('chai')
// const exec = require('child_process').exec
const packageJson = require('../package.json')
const sinon = require('sinon')
const chalk = require('chalk')
const AWS = require('aws-sdk-mock')

const expect = chai.expect
chai.should()

delete process.env.AWS_ACCESS_KEY_ID
delete process.env.AWS_SECRET_ACCESS_KEY

var sandbox
beforeEach(function () {
  sandbox = sinon.sandbox.create()
  sandbox.stub(process.stdout, 'write')
  sandbox.stub(process.stderr, 'write')
})

afterEach(function () {
  sandbox.restore()
  AWS.restore()
})

function cliTest (command, success, failure) {
  const cli = require('../src/cli2')
  const qrlCache = require('../src/qrlCache')
  function sandboxRestore (value) {
    sandbox.restore()
    qrlCache.clear()
    return value
  }
  return function (done) {
    cli
      .run(command)
      .then(function (result) {
        success = success || (result => result)
        const stdout = chalk.stripColor(process.stdout.write.args.reduce((a, b) => a + b, ''))
        const stderr = chalk.stripColor(process.stderr.write.args.reduce((a, b) => a + b, ''))
        return success(result, stdout, stderr)
      })
      .catch(function (err) {
        failure = failure || (err => { throw err })
        const stdout = chalk.stripColor(process.stdout.write.args.reduce((a, b) => a + b, ''))
        const stderr = chalk.stripColor(process.stderr.write.args.reduce((a, b) => a + b, ''))
        return failure(err, stdout, stderr)
      })
      .then(sandboxRestore)
      .catch(sandboxRestore)
      .then(done)
      .catch(done)
  }
}

describe('cli', function () {
  // Root command
  describe('qdone', function () {
    it('should print usage and exit 0',
      cliTest([], function (result, stdout, stderr) {
        expect(stdout).to.contain('usage: ')
      }))
  })

  describe('qdone --help', function () {
    it('should print usage and exit 0',
      cliTest(['--help'], function (result, stdout, stderr) {
        expect(stdout).to.contain('usage: ')
      }))
  })

  describe('qdone --version', function () {
    it('should print package.json version and exit 0',
      cliTest(['--version'], function (result, stdout, stderr) {
        expect(stdout).to.contain(packageJson.version)
      }))
  })

  describe('qdone --some-invalid-option', function () {
    it('should print usage and exit 1',
      cliTest(['--some-invalid-option'], null, function (err, stdout, stderr) {
        expect(stdout).to.contain('usage: ')
        expect(err).to.be.an('error')
      }))
  })

  // Enqueue
  describe('qdone enqueue', function () {
    it('should print usage and exit 1 with error',
      cliTest(['enqueue'], null, function (err, stdout, stderr) {
        expect(stdout).to.contain('usage: ')
        expect(stderr).to.contain('<queue>')
        expect(err).to.be.an('error')
      }))
  })

  describe('qdone enqueue --help', function () {
    it('should print usage and exit 0',
      cliTest(['enqueue', '--help'], function (result, stdout, stderr) {
        expect(stdout).to.contain('usage: ')
        expect(stdout).to.contain('enqueue')
      }))
  })

  describe('qdone enqueue onlyQueue', function () {
    it('should print usage and exit 1 with error',
      cliTest(['enqueue', 'onlyQueue'], null, function (err, stdout, stderr) {
        expect(stdout).to.contain('usage: ')
        expect(stderr).to.contain('<queue>')
        expect(err).to.be.an('error')
      }))
  })

  describe('qdone enqueue testQueue true # (with no credentials)', function () {
    before(function () {
      AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
        const err = new Error('Access to the resource https://sqs.us-east-1.amazonaws.com/ is denied.')
        err.code = 'AccessDenied'
        callback(err)
      })
    })
    it('should print usage and exit 1 with error',
      cliTest(['enqueue', 'testQueue', 'true'], null, function (err, stdout, stderr) {
        expect(stdout).to.contain('You must provide')
        expect(stderr).to.contain('Access to the resource https://sqs.us-east-1.amazonaws.com/ is denied.')
        expect(err).to.be.an('error')
      }))
  })

  describe('qdone enqueue testQueue true # (queue exists)', function () {
    before(function () {
      AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
        if (params.QueueName === 'qdone_testQueue') {
          callback(null, {QueueUrl: 'https://q.amazonaws.com/123456789101/qdone_testQueue'})
        } else if (params.QueueName === 'qdone_testQueue_failed') {
          callback(null, {QueueUrl: 'https://q.amazonaws.com/123456789102/qdone_testQueue_failed'})
        }
      })
      AWS.mock('SQS', 'sendMessage', function (params, callback) {
        callback(null, {
          MD5OfMessageAttributes: '00484c68...59e48f06',
          MD5OfMessageBody: '51b0a325...39163aa0',
          MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE'
        })
      })
    })
    it('should print id of enqueued message and exit 0',
      cliTest(['enqueue', 'testQueue', 'true'], function (result, stdout, stderr) {
        expect(stderr).to.contain('Enqueued job da68f62c-0c07-4bee-bf5f-7e856EXAMPLE')
      }))
  })

  describe('qdone enqueue testQueue true # (queue does not exist)', function () {
    before(function () {
      AWS.mock('SQS', 'getQueueUrl', function (params, callback) {
        const err = new Error('Queue does not exist.')
        err.code = 'AWS.SimpleQueueService.NonExistentQueue'
        callback(err)
      })
      AWS.mock('SQS', 'createQueue', function (params, callback) {
        if (params.QueueName === 'qdone_testQueue') {
          callback(null, {QueueUrl: 'https://q.amazonaws.com/123456789101/qdone_testQueue'})
        } else if (params.QueueName === 'qdone_testQueue_failed') {
          callback(null, {QueueUrl: 'https://q.amazonaws.com/123456789102/qdone_testQueue_failed'})
        }
      })
      AWS.mock('SQS', 'getQueueAttributes', function (params, callback) {
        if (params.QueueUrl === 'https://q.amazonaws.com/123456789102/qdone_testQueue_failed') {
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
              RedrivePolicy: '{\'deadLetterTargetArn\':\'arn:aws:sqs:us-east-1:80398EXAMPLE:qdone_testQueue_failed\',\'maxReceiveCount\':1000}',
              VisibilityTimeout: '30'
            }
          })
        }
      })
      AWS.mock('SQS', 'sendMessage', function (params, callback) {
        callback(null, {
          MD5OfMessageAttributes: '00484c68...59e48f06',
          MD5OfMessageBody: '51b0a325...39163aa0',
          MessageId: 'da68f62c-0c07-4bee-bf5f-7e856EXAMPLE'
        })
      })
    })
    it('should create queues, print the id of enqueued message and exit 0',
      cliTest(['enqueue', 'testQueue', 'true'], function (result, stdout, stderr) {
        expect(stderr).to.contain('Creating fail queue testQueue_failed')
        expect(stderr).to.contain('Looking up attributes for https://q.amazonaws.com/123456789102/qdone_testQueue_failed')
        expect(stderr).to.contain('Creating queue testQueue')
        expect(stderr).to.contain('Enqueued job da68f62c-0c07-4bee-bf5f-7e856EXAMPLE')
      }))
  })
})

/*
describe('cli worker', function () {
  describe('qdone worker', function () {
    it('should print error and exit 1', function () {
      try {
        process.argv.push('worker')
        cli.run(process.argv)
        expect(process.stderr.write.args.reduce((a, b) => a + b, '')).to.contain('missing required argument')
        expect(process.exit.args[0][0]).to.equal(1)
      } finally {
        sandbox.restore()
      }
    })
  })

  describe('qdone worker --help', function () {
    it('should return help and exit 0', function () {
      try {
        process.argv.push('worker')
        process.argv.push('--help')
        cli.run(process.argv)
        expect(process.stdout.write.args.reduce((a, b) => a + b, '')).to.contain('Usage: ')
        expect(process.exit.args[0][0]).to.equal(0)
      } finally {
        sandbox.restore()
      }
    })
  })
})
*/
