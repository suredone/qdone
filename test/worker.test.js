import { jest } from '@jest/globals'
import fs from 'fs'
import os from 'os'
import path from 'path'
import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand
} from '@aws-sdk/client-sqs'
import { mockClient } from 'aws-sdk-client-mock'
import 'aws-sdk-client-mock-jest'

import { executeJob } from '../src/worker.js'
import { getSQSClient, setSQSClient } from '../src/sqs.js'
import { getOptionsWithDefaults } from '../src/defaults.js'
import { shutdownCache } from '../src/cache.js'
import { _resetAllowlistCache } from '../src/commandPolicy.js'

getSQSClient()
const client = getSQSClient()

// Provide a mock Redis so dedupSuccessfullyProcessed doesn't throw
const cacheOptions = {
  cacheUri: 'redis://localhost',
  Redis: (await import('ioredis-mock')).default
}

describe('executeJob', () => {
  let sqsMock

  beforeEach(() => {
    shutdownCache()
    sqsMock = mockClient(client)
    setSQSClient(sqsMock)
    sqsMock.on(DeleteMessageCommand).resolves({})
    sqsMock.on(ChangeMessageVisibilityCommand).resolves({})
  })

  afterEach(() => {
    sqsMock.restore()
  })

  afterAll(shutdownCache)

  test('succeeds for a simple command', async () => {
    const job = {
      Body: 'true',
      MessageId: 'test-msg-simple',
      ReceiptHandle: 'test-receipt',
      Attributes: {
        ApproximateReceiveCount: '1',
        SentTimestamp: '1700000000000',
        ApproximateFirstReceiveTimestamp: '1700000001000'
      }
    }
    const qname = 'qdone_testqueue'
    const qrl = 'https://sqs.us-east-1.amazonaws.com/123456/qdone_testqueue'
    const opt = getOptionsWithDefaults({ ...cacheOptions, killAfter: 30 })

    const result = await executeJob(job, qname, qrl, opt)
    expect(result.jobsSucceeded).toBe(1)
    expect(result.jobsFailed).toBe(0)
  })

  test('handles missing Attributes object gracefully', async () => {
    const job = {
      Body: 'true',
      MessageId: 'test-msg-no-attrs',
      ReceiptHandle: 'test-receipt-2'
    }
    const qname = 'qdone_testqueue'
    const qrl = 'https://sqs.us-east-1.amazonaws.com/123456/qdone_testqueue'
    const opt = getOptionsWithDefaults({ ...cacheOptions, killAfter: 30 })

    const result = await executeJob(job, qname, qrl, opt)
    expect(result.jobsSucceeded).toBe(1)
    expect(result.jobsFailed).toBe(0)
  })

  test('SQS_RECEIVE_COUNT env var is available in child process', async () => {
    const job = {
      Body: 'node -e "if(process.env.SQS_RECEIVE_COUNT !== \'3\') process.exit(1)"',
      MessageId: 'test-msg-rc',
      ReceiptHandle: 'test-receipt-rc',
      Attributes: {
        ApproximateReceiveCount: '3'
      }
    }
    const qname = 'qdone_testqueue'
    const qrl = 'https://sqs.us-east-1.amazonaws.com/123456/qdone_testqueue'
    const opt = getOptionsWithDefaults({ ...cacheOptions, killAfter: 30 })

    const result = await executeJob(job, qname, qrl, opt)
    expect(result.jobsSucceeded).toBe(1)
    expect(result.jobsFailed).toBe(0)
  })

  test('SQS_RECEIVE_COUNT defaults to 1 when attribute is missing', async () => {
    const job = {
      Body: 'node -e "if(process.env.SQS_RECEIVE_COUNT !== \'1\') process.exit(1)"',
      MessageId: 'test-msg-rc-default',
      ReceiptHandle: 'test-receipt-rc-default',
      Attributes: {}
    }
    const qname = 'qdone_testqueue'
    const qrl = 'https://sqs.us-east-1.amazonaws.com/123456/qdone_testqueue'
    const opt = getOptionsWithDefaults({ ...cacheOptions, killAfter: 30 })

    const result = await executeJob(job, qname, qrl, opt)
    expect(result.jobsSucceeded).toBe(1)
    expect(result.jobsFailed).toBe(0)
  })

  test('QDONE_QUEUE_NAME env var is available in child process', async () => {
    const job = {
      Body: 'node -e "if(process.env.QDONE_QUEUE_NAME !== \'qdone_testqueue\') process.exit(1)"',
      MessageId: 'test-msg-qn',
      ReceiptHandle: 'test-receipt-qn',
      Attributes: {}
    }
    const qname = 'qdone_testqueue'
    const qrl = 'https://sqs.us-east-1.amazonaws.com/123456/qdone_testqueue'
    const opt = getOptionsWithDefaults({ ...cacheOptions, killAfter: 30 })

    const result = await executeJob(job, qname, qrl, opt)
    expect(result.jobsSucceeded).toBe(1)
    expect(result.jobsFailed).toBe(0)
  })

  test('SQS_MESSAGE_ID env var is available in child process', async () => {
    const job = {
      Body: 'node -e "if(process.env.SQS_MESSAGE_ID !== \'msg-id-test\') process.exit(1)"',
      MessageId: 'msg-id-test',
      ReceiptHandle: 'test-receipt-mid',
      Attributes: {}
    }
    const qname = 'qdone_testqueue'
    const qrl = 'https://sqs.us-east-1.amazonaws.com/123456/qdone_testqueue'
    const opt = getOptionsWithDefaults({ ...cacheOptions, killAfter: 30 })

    const result = await executeJob(job, qname, qrl, opt)
    expect(result.jobsSucceeded).toBe(1)
    expect(result.jobsFailed).toBe(0)
  })

  test('SQS_SENT_TIMESTAMP env var is available in child process', async () => {
    const job = {
      Body: 'node -e "if(process.env.SQS_SENT_TIMESTAMP !== \'1700000000000\') process.exit(1)"',
      MessageId: 'test-msg-st',
      ReceiptHandle: 'test-receipt-st',
      Attributes: {
        SentTimestamp: '1700000000000'
      }
    }
    const qname = 'qdone_testqueue'
    const qrl = 'https://sqs.us-east-1.amazonaws.com/123456/qdone_testqueue'
    const opt = getOptionsWithDefaults({ ...cacheOptions, killAfter: 30 })

    const result = await executeJob(job, qname, qrl, opt)
    expect(result.jobsSucceeded).toBe(1)
    expect(result.jobsFailed).toBe(0)
  })

  test('SQS_FIRST_RECEIVE_TIMESTAMP env var is available in child process', async () => {
    const job = {
      Body: 'node -e "if(process.env.SQS_FIRST_RECEIVE_TIMESTAMP !== \'1700000001000\') process.exit(1)"',
      MessageId: 'test-msg-frt',
      ReceiptHandle: 'test-receipt-frt',
      Attributes: {
        ApproximateFirstReceiveTimestamp: '1700000001000'
      }
    }
    const qname = 'qdone_testqueue'
    const qrl = 'https://sqs.us-east-1.amazonaws.com/123456/qdone_testqueue'
    const opt = getOptionsWithDefaults({ ...cacheOptions, killAfter: 30 })

    const result = await executeJob(job, qname, qrl, opt)
    expect(result.jobsSucceeded).toBe(1)
    expect(result.jobsFailed).toBe(0)
  })

  test('SQS_MESSAGE_GROUP_ID env var is available for FIFO messages', async () => {
    const job = {
      Body: 'node -e "if(process.env.SQS_MESSAGE_GROUP_ID !== \'my-group\') process.exit(1)"',
      MessageId: 'test-msg-fifo',
      ReceiptHandle: 'test-receipt-fifo',
      Attributes: {
        ApproximateReceiveCount: '1',
        MessageGroupId: 'my-group'
      }
    }
    const qname = 'qdone_testqueue.fifo'
    const qrl = 'https://sqs.us-east-1.amazonaws.com/123456/qdone_testqueue.fifo'
    const opt = getOptionsWithDefaults({ ...cacheOptions, killAfter: 30 })

    const result = await executeJob(job, qname, qrl, opt)
    expect(result.jobsSucceeded).toBe(1)
    expect(result.jobsFailed).toBe(0)
  })

  test('SQS_MESSAGE_GROUP_ID defaults to empty string for non-FIFO', async () => {
    const job = {
      Body: 'node -e "if(process.env.SQS_MESSAGE_GROUP_ID !== \'\') process.exit(1)"',
      MessageId: 'test-msg-no-group',
      ReceiptHandle: 'test-receipt-no-group',
      Attributes: {
        ApproximateReceiveCount: '1'
      }
    }
    const qname = 'qdone_testqueue'
    const qrl = 'https://sqs.us-east-1.amazonaws.com/123456/qdone_testqueue'
    const opt = getOptionsWithDefaults({ ...cacheOptions, killAfter: 30 })

    const result = await executeJob(job, qname, qrl, opt)
    expect(result.jobsSucceeded).toBe(1)
    expect(result.jobsFailed).toBe(0)
  })

  describe('command policy', () => {
    const allowlistPath = path.join(os.tmpdir(), 'qdone-test-allowlist.json')
    const qname = 'qdone_testqueue'
    const qrl = 'https://sqs.us-east-1.amazonaws.com/123456/qdone_testqueue'
    const PWN = path.join(os.tmpdir(), 'qdone-pwn-marker')

    beforeEach(() => {
      _resetAllowlistCache()
      // node is allowed with the `-e` subcommand so we can exec real subprocesses.
      fs.writeFileSync(allowlistPath, JSON.stringify({ version: 1, binaries: { node: { subcommands: ['-e'] } } }))
      if (fs.existsSync(PWN)) fs.unlinkSync(PWN)
    })

    test('off mode runs everything unchanged via the shell (honors &&)', async () => {
      const job = { Body: 'echo a && echo b', MessageId: 'p-off', ReceiptHandle: 'r', Attributes: { ApproximateReceiveCount: '1' } }
      const opt = getOptionsWithDefaults({ ...cacheOptions, killAfter: 30, commandPolicy: 'off' })
      const result = await executeJob(job, qname, qrl, opt)
      expect(result.jobsSucceeded).toBe(1)
    })

    test('audit mode logs a violation but still runs the job', async () => {
      const logSpy = jest.spyOn(console, 'log').mockImplementation(() => {})
      const job = { Body: 'echo a; echo b', MessageId: 'p-audit', ReceiptHandle: 'r', Attributes: { ApproximateReceiveCount: '1' } }
      const opt = getOptionsWithDefaults({ ...cacheOptions, killAfter: 30, commandPolicy: 'audit', commandAllowlistFile: allowlistPath })
      const result = await executeJob(job, qname, qrl, opt)
      expect(result.jobsSucceeded).toBe(1)
      expect(logSpy.mock.calls.flat().join(' ')).toMatch(/COMMAND_POLICY_VIOLATION/)
      logSpy.mockRestore()
    })

    test('enforce mode rejects a violation without running or deleting it', async () => {
      const job = { Body: 'echo a; echo b', MessageId: 'p-enf-rej', ReceiptHandle: 'r', Attributes: { ApproximateReceiveCount: '1' } }
      const opt = getOptionsWithDefaults({ ...cacheOptions, killAfter: 30, commandPolicy: 'enforce', commandAllowlistFile: allowlistPath })
      const result = await executeJob(job, qname, qrl, opt)
      expect(result.jobsFailed).toBe(1)
      expect(sqsMock).not.toHaveReceivedCommand(DeleteMessageCommand)
    })

    test('enforce mode runs a valid command via execFile (no shell) and deletes on success', async () => {
      const job = { Body: "node -e 'process.exit(0)'", MessageId: 'p-enf-ok', ReceiptHandle: 'r', Attributes: { ApproximateReceiveCount: '1' } }
      const opt = getOptionsWithDefaults({ ...cacheOptions, killAfter: 30, commandPolicy: 'enforce', commandAllowlistFile: allowlistPath })
      const result = await executeJob(job, qname, qrl, opt)
      expect(result.jobsSucceeded).toBe(1)
      expect(sqsMock).toHaveReceivedCommand(DeleteMessageCommand)
    })

    test('enforce: a quoted command-substitution arg is inert (execFile passes it literally)', async () => {
      const job = { Body: "node -e '' '$(touch " + PWN + ")'", MessageId: 'p-enf-inert', ReceiptHandle: 'r', Attributes: { ApproximateReceiveCount: '1' } }
      const opt = getOptionsWithDefaults({ ...cacheOptions, killAfter: 30, commandPolicy: 'enforce', commandAllowlistFile: allowlistPath })
      const result = await executeJob(job, qname, qrl, opt)
      expect(result.jobsSucceeded).toBe(1)
      expect(fs.existsSync(PWN)).toBe(false) // proof: no shell ran the $()
    })

    test('enforce: missing allowlist file fails OPEN (runs via shell) with a misconfig alert', async () => {
      const logSpy = jest.spyOn(console, 'log').mockImplementation(() => {})
      const job = { Body: 'echo hello', MessageId: 'p-misconfig', ReceiptHandle: 'r', Attributes: { ApproximateReceiveCount: '1' } }
      const opt = getOptionsWithDefaults({ ...cacheOptions, killAfter: 30, commandPolicy: 'enforce', commandAllowlistFile: '/no/such/file.json' })
      const result = await executeJob(job, qname, qrl, opt)
      expect(result.jobsSucceeded).toBe(1)
      expect(logSpy.mock.calls.flat().join(' ')).toMatch(/COMMAND_POLICY_MISCONFIG/)
      logSpy.mockRestore()
    })

    test('non-zero exit fails in both shell (off) and execFile (enforce) paths', async () => {
      let opt = getOptionsWithDefaults({ ...cacheOptions, killAfter: 30, commandPolicy: 'off' })
      let r = await executeJob({ Body: 'false', MessageId: 'p-exit-shell', ReceiptHandle: 'r', Attributes: { ApproximateReceiveCount: '1' } }, qname, qrl, opt)
      expect(r.jobsFailed).toBe(1)

      opt = getOptionsWithDefaults({ ...cacheOptions, killAfter: 30, commandPolicy: 'enforce', commandAllowlistFile: allowlistPath })
      r = await executeJob({ Body: "node -e 'process.exit(3)'", MessageId: 'p-exit-execfile', ReceiptHandle: 'r', Attributes: { ApproximateReceiveCount: '1' } }, qname, qrl, opt)
      expect(r.jobsFailed).toBe(1)
    })

    test('child env carries SQS attributes in the execFile path', async () => {
      const job = { Body: "node -e 'process.exit(process.env.QDONE_QUEUE_NAME ? 0 : 1)'", MessageId: 'p-env', ReceiptHandle: 'r', Attributes: { ApproximateReceiveCount: '1' } }
      const opt = getOptionsWithDefaults({ ...cacheOptions, killAfter: 30, commandPolicy: 'enforce', commandAllowlistFile: allowlistPath })
      const result = await executeJob(job, qname, qrl, opt)
      expect(result.jobsSucceeded).toBe(1)
    })
  })
})
