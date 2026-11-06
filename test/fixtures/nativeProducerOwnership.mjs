import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { mkdtemp, lstat, rm } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { join, resolve } from 'node:path'
import { tmpdir } from 'node:os'
import { pathToFileURL } from 'node:url'
import Redis from 'ioredis'

const [packagePath = '.', format = 'esm', mode = 'all'] = process.argv.slice(2)
assert.ok(['esm', 'cjs'].includes(format))
assert.ok(['all', 'retry', 'stale'].includes(mode))
const root = resolve(packagePath)
const tree = format === 'cjs' ? join(root, 'commonjs') : root
const load = async name => {
  const module = await import(pathToFileURL(join(tree, name)))
  return module.default ?? module
}
const { enqueue, enqueueBatch, EnqueueAcceptanceError } = await load('index.js')
const { formatMessage } = await load('src/enqueue.js')
const { setSQSClient } = await load('src/sqs.js')
const { getOptionsWithDefaults } = await load('src/defaults.js')
const { resetCache } = await load('src/cache.js')
const { JobExecutor } = await load('src/scheduler/jobExecutor.js')
const { executeJob } = format === 'esm' ? await load('src/worker.js') : {}
const { qrlCacheClear } = await load('src/qrlCache.js')
const { dedupSuccessfullyProcessed, dedupSuccessfullyProcessedMulti, getDeduplicationId, getCacheKey } = await load('src/dedup.js')
const { QueueDoesNotExist } = createRequire(join(root, 'package.json'))('@aws-sdk/client-sqs')

const scratch = await mkdtemp(join(tmpdir(), 'qo-'))
const socket = join(scratch, 'r.sock')
const server = spawn('redis-server', [
  '--port', '0', '--unixsocket', socket, '--unixsocketperm', '600',
  '--save', '', '--appendonly', 'no', '--daemonize', 'no', '--dir', scratch
], { stdio: ['ignore', 'ignore', 'inherit'] })
const finished = once(server, 'exit')
finished.catch(() => {})
const redis = new Redis(socket, { lazyConnect: true, retryStrategy: () => null, commandTimeout: 2000 })
redis.on('error', () => {})
const delay = ms => new Promise(resolve => setTimeout(resolve, ms))
const successful = Entries => Entries.map(({ Id }) => ({ Id, MessageId: 'ack-' + Id }))
const failure = Id => ({ Id, Code: 'InvalidParameterValue', SenderFault: true })
const batch = (Entries, attempts = 1) => ({ Successful: successful(Entries), $metadata: { attempts } })
const opts = (name, extra = {}) => ({
  prefix: 'native_',
  externalDedup: true,
  dedupProtocol: 'owner-v1',
  dedupPeriod: 30,
  cacheUri: 'redis://unused.invalid',
  cachePrefix: 'native:' + name + ':',
  Redis: function TestRedis () { return redis },
  sendRetries: 1,
  disableLog: true,
  ...extra
})
const key = (command, opt) => getCacheKey(getDeduplicationId(command, opt), opt)
const jobs = (...commands) => commands.map(command => ({ queue: 'jobs', command }))
const outcomes = err => Object.fromEntries(err.outcomes.map(({ Id, status }) => [Id, status]))
const assertTyped = err => {
  assert.ok(err instanceof EnqueueAcceptanceError)
  assert.doesNotMatch(err.message, /unused\.invalid|synthetic-command|owner-v1:/)
  return err
}
let sendCalls, single, sendBatch, deleteSingle, deleteBatch, getQueue
function resetBroker () {
  qrlCacheClear()
  sendCalls = []
  single = async () => ({ MessageId: 'single-ack', $metadata: { attempts: 1 } })
  sendBatch = async ({ Entries }) => batch(Entries)
  deleteSingle = async () => ({})
  deleteBatch = async ({ Entries }) => ({ Successful: Entries.map(({ Id }) => ({ Id })) })
  getQueue = async ({ QueueName }) => ({ QueueUrl: 'https://sqs.example.invalid/' + QueueName })
  setSQSClient({
    async send ({ constructor: { name }, input }) {
      switch (name) {
        case 'GetQueueUrlCommand': return getQueue(input)
        case 'SendMessageCommand': sendCalls.push(structuredCloneInput(input)); return single(input)
        case 'SendMessageBatchCommand': sendCalls.push(structuredCloneInput(input)); return sendBatch(input)
        case 'ChangeMessageVisibilityCommand': return {}
        case 'DeleteMessageCommand': return deleteSingle(input)
        case 'DeleteMessageBatchCommand': return deleteBatch(input)
        default: throw new Error('Unexpected synthetic command ' + name)
      }
    }
  })
}

function structuredCloneInput (input) {
  return JSON.parse(JSON.stringify(input))
}

async function within (promise, name) {
  let timer
  try {
    return await Promise.race([promise, new Promise((resolve, reject) => {
      timer = setTimeout(() => reject(new Error('Timed out waiting for ' + name)), 4000)
    })])
  } finally {
    clearTimeout(timer)
  }
}

function gate () {
  let release
  const promise = new Promise(resolve => { release = resolve })
  return { promise, release }
}

let cases = 0
async function test (name, check) {
  resetBroker()
  await check()
  cases++
  console.log(JSON.stringify({ case: name, status: 'pass' }))
}

async function retryWitness () {
  const opt = opts('retry')
  sendBatch = async ({ Entries }) => ({
    Successful: successful(Entries.slice(0, 1)),
    Failed: [failure(Entries[1].Id)],
    $metadata: { attempts: 1 }
  })
  const firstError = await enqueueBatch(jobs('accepted', 'definitive-rejection'), opt).then(() => null, error => error)
  assert.ok(firstError, 'a partial broker response must reject')
  sendBatch = async ({ Entries }) => batch(Entries)
  const retry = await enqueueBatch(jobs('accepted', 'definitive-rejection'), opt)
  assert.equal(sendCalls.length, 2, 'retry must actually SEND the earlier rejected entry')
  assert.deepEqual(sendCalls[1].Entries.map(({ Id }) => Id), ['1'])
  assert.equal(retry.numFlushed, 2)
  assert.equal(retry.results[0].MessageId, 'ack-0')
  assert.equal(retry.results[0].Deduplicated, 'confirmed')
  assertTyped(firstError)
  assert.deepEqual(outcomes(firstError), { 0: 'accepted', 1: 'rejected' })
  assert.equal(firstError.numFlushed, 1)
  const duplicates = await enqueueBatch(jobs('accepted', 'definitive-rejection'), opt)
  assert.deepEqual(duplicates.results.map(({ MessageId, Deduplicated }) => [MessageId, Deduplicated]), [
    ['ack-0', 'confirmed'], ['ack-1', 'confirmed']
  ])
  assert.equal(sendCalls.length, 2)
}

async function staleWitness () {
  const opt = opts('stale')
  const command = 'generation-rollover'
  const cacheKey = key(command, opt)
  await enqueue('jobs', command, opt)
  const old = sendCalls[0]
  await redis.pexpire(cacheKey, 1)
  await delay(20)
  assert.equal(await redis.exists(cacheKey), 0)
  await enqueue('jobs', command, opt)
  const value = await redis.get(cacheKey)
  assert.ok(value)
  assert.equal(await dedupSuccessfullyProcessed(old, opt), 0, 'stale consumer must not release the newer owner')
  assert.equal(await redis.get(cacheKey), value)
  assert.equal(await dedupSuccessfullyProcessed(sendCalls[1], opt), 1)
}

try {
  for (let i = 0; i < 100; i++) {
    const ready = await lstat(socket).catch(error => {
      if (error.code !== 'ENOENT') throw error
    })
    if (ready?.isSocket()) break
    if (server.exitCode !== null || !server.pid) throw new Error('Private Redis did not start')
    await delay(10)
  }
  await redis.connect()
  assert.equal(await redis.ping(), 'PONG')
  assert.deepEqual(await redis.config('GET', 'port'), ['port', '0'])

  if (mode !== 'stale') await test('partial retry actually sends rejected input', retryWitness)
  if (mode !== 'retry') await test('stale release after expiry', staleWitness)
  if (mode === 'all') {
    await test('atomic acquisition, pending competitor and confirmed TTL', async () => {
      const opt = opts('atomic', { fifo: true, groupId: 'test-group', delay: 4 })
      const entered = gate()
      const reply = gate()
      single = async () => {
        entered.release()
        await within(reply.promise, 'producer response release')
        return { MessageId: 'actual-confirmed', SequenceNumber: '12345' }
      }
      const first = enqueue('jobs', 'atomic-claim', opt)
      first.catch(() => {})
      try {
        await within(entered.promise, 'producer SEND')
        const cacheKey = key('atomic-claim', opt)
        const ttl = await redis.pttl(cacheKey)
        assert.ok(ttl > 0 && ttl <= 30000)
        await delay(20)
        await assert.rejects(enqueue('jobs', 'atomic-claim', { ...opt, dedupPeriod: 90 }),
          err => (assertTyped(err).outcomes[0].reason === 'pending-owner'))
        const pendingTtl = await redis.pttl(cacheKey)
        assert.ok(pendingTtl < ttl)
        reply.release()
        assert.deepEqual(await first, { MessageId: 'actual-confirmed', SequenceNumber: '12345' })
        const confirmedTtl = await redis.pttl(cacheKey)
        assert.ok(confirmedTtl > 0 && confirmedTtl <= pendingTtl)
        assert.deepEqual(await enqueue('jobs', 'atomic-claim', opt), { MessageId: 'actual-confirmed', Deduplicated: 'confirmed' })
        assert.equal(sendCalls.length, 1)
        assert.equal(sendCalls[0].MessageGroupId, 'test-group')
        assert.equal(sendCalls[0].DelaySeconds, 4)
      } finally {
        reply.release()
        await first.catch(() => {})
      }
    })

    await test('aliases across queues and chunks retain one actual owner', async () => {
      const opt = opts('aliases', { fifo: true })
      const pairs = jobs(...Array.from({ length: 12 }, (_, i) => 'alias-' + i))
      pairs[10].messageOptions = { deduplicationId: 'alias-0' }
      pairs[11] = { queue: 'different', command: 'alias-0' }
      const result = await enqueueBatch(pairs, opt)
      assert.equal(result.numFlushed, 12)
      assert.equal(sendCalls.length, 1)
      assert.equal(sendCalls[0].Entries.length, 10)
      assert.deepEqual(result.results.slice(10).map(({ Id, MessageId, Deduplicated }) => [Id, MessageId, Deduplicated]),
        [['10', 'ack-0', 'confirmed'], ['11', 'ack-0', 'confirmed']])
      resetBroker()
      const simultaneous = await enqueueBatch([
        { queue: 'one', command: 'parallel-alias' },
        { queue: 'two', command: 'parallel-alias' }
      ], opt)
      assert.equal(sendCalls.length, 1)
      assert.equal(simultaneous.results[1].Deduplicated, 'confirmed')
    })

    await test('failure stops further chunks and records untouched input', async () => {
      const opt = opts('stop')
      const pairs = jobs(...Array.from({ length: 14 }, (_, i) => 'chunk-' + i))
      pairs[0].queue = 'buffered'
      sendBatch = async ({ Entries }) => ({
        Successful: successful(Entries.slice(0, -1)), Failed: [failure(Entries[9].Id)], $metadata: { attempts: 1 }
      })
      await assert.rejects(enqueueBatch(pairs, opt), err => {
        assertTyped(err)
        assert.deepEqual(err.outcomes.map(({ status }) => status), [
          'notAttempted', ...Array(9).fill('accepted'), 'rejected', ...Array(3).fill('notAttempted')
        ])
        return true
      })
      assert.equal(sendCalls.length, 1)
      assert.equal(await redis.exists(key('chunk-0', opt)), 0)
    })

    await test('parallel tail flush retains already-started positive results', async () => {
      const opt = opts('tail')
      const slow = gate()
      const started = gate()
      const fail = gate()
      let settled = false
      sendBatch = async ({ Entries }) => {
        if (Entries[0].Id === '0') {
          await within(started.promise, 'second tail send')
          fail.release()
          return { Failed: [failure('0')], $metadata: { attempts: 1 } }
        }
        started.release()
        await within(slow.promise, 'tail response release')
        return batch(Entries)
      }
      const result = enqueueBatch([{ queue: 'a', command: 'a' }, { queue: 'b', command: 'b' }], opt)
        .then(() => { settled = true; assert.fail('partial success') }, error => { settled = true; return error })
      try {
        await within(fail.promise, 'both tail sends')
        await delay(20)
        assert.equal(settled, false)
      } finally {
        slow.release()
      }
      const error = await result
      assertTyped(error)
      assert.deepEqual(outcomes(error), { 0: 'rejected', 1: 'accepted' })
      assert.equal(error.results[0].Id, '1')
    })

    await test('malformed classifications retain positives without cleanup', async () => {
      const opt = opts('protocol')
      sendBatch = async () => ({
        Successful: [
          { Id: '0', MessageId: 'positive' }, { Id: '1', MessageId: 'conflict' },
          { Id: '3' }, { Id: 'foreign', MessageId: 'extra' }
        ],
        Failed: [failure('1'), failure('2')],
        $metadata: { attempts: 1 }
      })
      await assert.rejects(enqueueBatch(jobs('p0', 'p1', 'p2', 'p3', 'p4'), opt), err => {
        assertTyped(err)
        assert.equal(err.protocolFailure, true)
        assert.deepEqual(outcomes(err), { 0: 'accepted', 1: 'unknown', 2: 'unknown', 3: 'unknown', 4: 'unknown' })
        return true
      })
      assert.match(await redis.get(key('p0', opt)), /:accepted:positive$/)
      for (const command of ['p1', 'p2', 'p3', 'p4']) {
        assert.match(await redis.get(key(command, opt)), /:pending:$/)
      }
      assert.equal(sendCalls.length, 1)
    })

    await test('unknown attempts retain ownership and cannot certify duplicates', async () => {
      const opt = opts('unknown')
      for (const attempts of [undefined, 2]) {
        const command = 'sdk-' + attempts
        sendBatch = async () => ({ Failed: [failure('0')], $metadata: { attempts } })
        await assert.rejects(enqueueBatch(jobs(command), opt),
          err => (assertTyped(err).outcomes[0].status === 'unknown'))
        const calls = sendCalls.length
        await assert.rejects(enqueueBatch(jobs(command, 'untouched-' + attempts), opt),
          err => (assertTyped(err).outcomes[0].reason === 'pending-owner' && err.outcomes[1].status === 'notAttempted'))
        assert.equal(sendCalls.length, calls)
        assert.equal(await redis.exists(key('untouched-' + attempts, opt)), 0)
        assert.ok(await redis.pttl(key(command, opt)) > 0)
      }
      single = async () => { throw new Error('synthetic-commit-then-response-lost') }
      await assert.rejects(enqueue('jobs', 'send-lost', opt), err => (assertTyped(err).outcomes[0].status === 'unknown'))
      const count = sendCalls.length
      await assert.rejects(enqueue('jobs', 'send-lost', opt), err => err.outcomes[0].reason === 'pending-owner')
      assert.equal(sendCalls.length, count)
    })

    await test('queue recreation retains FIFO identity and earlier uncertainty', async () => {
      const opt = opts('recreation', { sendRetries: 2, fifo: true })
      let lookups = 0
      getQueue = async ({ QueueName }) => ({ QueueUrl: 'https://sqs.example.invalid/' + ++lookups + '/' + QueueName })
      sendBatch = async ({ Entries }) => {
        if (sendCalls.length === 1) {
          throw new QueueDoesNotExist({
            message: 'not found', $metadata: { attempts: 2, httpStatusCode: 400 }
          })
        }
        return { Failed: [failure(Entries[0].Id)], $metadata: { attempts: 1 } }
      }
      await assert.rejects(enqueueBatch(jobs('reroute'), opt), err => (assertTyped(err).outcomes[0].status === 'unknown'))
      assert.equal(lookups, 2)
      assert.equal(sendCalls.length, 2)
      assert.notEqual(sendCalls[0].QueueUrl, sendCalls[1].QueueUrl)
      assert.deepEqual(sendCalls[0].Entries, sendCalls[1].Entries)
      assert.ok(await redis.pttl(key('reroute', opt)) > 0)
    })

    await test('later chunks reuse the newly recreated queue URL', async () => {
      const opt = opts('later-chunk', { sendRetries: 2, fifo: true })
      let lookups = 0
      getQueue = async ({ QueueName }) => ({ QueueUrl: 'https://sqs.example.invalid/' + ++lookups + '/' + QueueName })
      sendBatch = async ({ Entries }) => {
        if (sendCalls.length === 1) {
          throw new QueueDoesNotExist({ message: 'not found', $metadata: { attempts: 1, httpStatusCode: 400 } })
        }
        return batch(Entries)
      }
      const result = await enqueueBatch(jobs(...Array.from({ length: 11 }, (_, i) => 'refreshed-' + i)), opt)
      assert.equal(result.numFlushed, 11)
      assert.equal(sendCalls.length, 3)
      assert.equal(lookups, 2)
      assert.notEqual(sendCalls[0].QueueUrl, sendCalls[1].QueueUrl)
      assert.deepEqual(sendCalls[0].Entries, sendCalls[1].Entries)
      assert.equal(sendCalls[2].QueueUrl, sendCalls[1].QueueUrl)
      assert.deepEqual(sendCalls[2].Entries.map(({ Id }) => Id), ['10'])
    })

    await test('owner attributes fit in the existing per-batch size budget', async () => {
      const opt = getOptionsWithDefaults(opts('size'))
      const firstOptions = { deduplicationId: 'x' }
      const secondOptions = { deduplicationId: 'y' }
      const overhead = JSON.stringify(formatMessage('', '0', opt, firstOptions)).length
      const command = 'x'.repeat(131072 - 20 - overhead)
      const result = await enqueueBatch([
        { queue: 'jobs', command, messageOptions: firstOptions },
        { queue: 'jobs', command, messageOptions: secondOptions }
      ], opt)
      assert.equal(result.numFlushed, 2)
      assert.deepEqual(sendCalls.map(({ Entries }) => Entries.length), [1, 1])
      for (const { Entries } of sendCalls) {
        assert.ok(Entries[0].MessageAttributes.QdoneDeduplicationOwner)
        assert.ok(JSON.stringify(Entries[0]).length < 262144)
      }
    })

    await test('claim response loss sends nothing and confirmation failure retains acceptance', async () => {
      const opt = opts('cache-errors')
      const evaluate = redis.eval.bind(redis)
      try {
        redis.eval = async (...args) => {
          await evaluate(...args)
          throw new Error('lost cache claim response')
        }
        await assert.rejects(enqueue('jobs', 'claim-lost', opt), err => (assertTyped(err).outcomes[0].reason === 'cache-claim'))
        assert.equal(sendCalls.length, 0)
        assert.ok(await redis.pttl(key('claim-lost', opt)) > 0)
        redis.eval = async (...args) => {
          if (args[0].includes('local key, token, messageId')) throw new Error('cache unavailable before confirm')
          return evaluate(...args)
        }
        const result = await enqueue('jobs', 'confirm-lost', opt)
        assert.equal(result.MessageId, 'single-ack')
        assert.equal(result.DeduplicationConfirmation, 'unavailable')
        assert.match(await redis.get(key('confirm-lost', opt)), /:pending:$/)
      } finally {
        redis.eval = evaluate
      }
    })

    await test('fast consumer cannot cause late confirmation to recreate ownership', async () => {
      const opt = opts('fast-consumer')
      single = async input => {
        assert.equal(await dedupSuccessfullyProcessed(input, opt), 1)
        return { MessageId: 'fast-accepted' }
      }
      const result = await enqueue('jobs', 'fast', opt)
      assert.equal(result.MessageId, 'fast-accepted')
      assert.equal(result.DeduplicationConfirmation, 'not-current')
      assert.equal(await redis.exists(key('fast', opt)), 0)
    })

    await test('late producer replies cannot mutate newer ownership', async () => {
      const opt = opts('late')
      for (const positive of [true, false]) {
        const command = 'late-' + positive
        const cacheKey = key(command, opt)
        const firstCall = sendCalls.length + 1
        let current
        sendBatch = async ({ Entries }) => {
          if (sendCalls.length !== firstCall) return batch(Entries)
          await redis.pexpire(cacheKey, 1)
          await delay(20)
          await enqueue('jobs', command, opt)
          current = await redis.get(cacheKey)
          return positive
            ? batch(Entries)
            : { Failed: [failure('0')], $metadata: { attempts: 1 } }
        }
        if (positive) {
          assert.equal((await enqueueBatch(jobs(command), opt)).results[0].DeduplicationConfirmation, 'not-current')
        } else {
          await assert.rejects(enqueueBatch(jobs(command), opt), err => err.outcomes[0].status === 'rejected')
        }
        assert.ok(current)
        assert.equal(await redis.get(cacheKey), current)
      }
    })

    await test('legacy mode, malformed records, and tokenless cleanup', async () => {
      const owner = opts('migration')
      const legacy = getOptionsWithDefaults({ ...owner, dedupProtocol: undefined })
      assert.equal(legacy.dedupProtocol, 'legacy')
      await enqueue('jobs', 'old-key', legacy)
      const old = sendCalls[0]
      const oldKey = key('old-key', owner)
      assert.equal(await redis.get(oldKey), '1')
      assert.ok(await redis.pttl(oldKey) > 0)
      assert.equal((await enqueue('jobs', 'old-key', legacy)).Deduplicated, 'legacy')
      await assert.rejects(enqueue('jobs', 'old-key', owner), err => err.outcomes[0].reason === 'legacy-owner')
      await enqueue('jobs', 'owner-key', owner)
      const owned = sendCalls[1]
      const ownedKey = key('owner-key', owner)
      const token = owned.MessageAttributes.QdoneDeduplicationOwner
      assert.equal(await dedupSuccessfullyProcessed({
        ...old, MessageAttributes: { ...old.MessageAttributes, QdoneDeduplicationOwner: token }
      }, owner), 0)
      assert.equal(await dedupSuccessfullyProcessed({
        ...owned, MessageAttributes: { QdoneDeduplicationId: owned.MessageAttributes.QdoneDeduplicationId }
      }, owner), 0)
      assert.equal((await enqueue('jobs', 'owner-key', legacy)).MessageId, 'single-ack')
      assert.equal(sendCalls.length, 2)
      await assert.rejects(redis.incr(ownedKey), /not an integer/)
      const broken = key('broken', owner)
      await redis.set(broken, 'owner-v1:malformed', 'EX', 30)
      await assert.rejects(enqueue('jobs', 'broken', legacy), err => err.outcomes[0].reason === 'invalid-owner')
      await redis.persist(oldKey)
      await assert.rejects(enqueue('jobs', 'old-key', owner), err => err.outcomes[0].reason === 'legacy-owner')
      assert.equal(await redis.pttl(oldKey), -1)
      assert.equal(await dedupSuccessfullyProcessedMulti([old, owned], legacy), 2)
      assert.equal(await redis.exists(ownedKey, oldKey), 0)
    })

    await test('native workers honor the exact owner after broker deletion', async () => {
      const opt = getOptionsWithDefaults(opts('worker', { fifo: true, killAfter: 10 }))
      const command = 'true'
      const cacheKey = key(command, opt)
      await enqueue('jobs', command, opt)
      const original = sendCalls[0]
      await redis.pexpire(cacheKey, 1)
      await delay(20)
      await enqueue('jobs', command, opt)
      const replacement = sendCalls[1]
      const qrl = replacement.QueueUrl
      const job = (sent, id) => ({
        MessageId: id,
        ReceiptHandle: 'receipt-' + id,
        Body: 'true',
        Attributes: { MessageGroupId: 'owned-native-group' },
        MessageAttributes: sent.MessageAttributes
      })
      if (executeJob) {
        assert.equal((await executeJob(job(original, 'legacy-old'), 'jobs', qrl, opt)).jobsSucceeded, 1)
        assert.ok(await redis.exists(cacheKey))
      }
      const executor = new JobExecutor(opt)
      try {
        await executor.executeJobs([job(original, 'executor-old')], async () => {}, 'jobs', qrl)
        await executor.maintainVisibility()
        assert.ok(await redis.exists(cacheKey))
        deleteBatch = async ({ Entries }) => ({ Failed: Entries.map(({ Id }) => failure(Id)) })
        await executor.executeJobs([job(replacement, 'executor-ack-failed')], async () => {}, 'jobs', qrl)
        await executor.maintainVisibility()
        assert.ok(await redis.exists(cacheKey))
        deleteBatch = async ({ Entries }) => ({ Successful: Entries.map(({ Id }) => ({ Id })) })
        await executor.executeJobs([job(replacement, 'executor-matching')], async () => {}, 'jobs', qrl)
        await executor.maintainVisibility()
        assert.equal(await redis.exists(cacheKey), 0)
      } finally {
        await executor.shutdown()
      }
      if (!executeJob) return
      await enqueue('jobs', command, opt)
      const later = sendCalls[2]
      deleteSingle = async () => { throw new Error('synthetic delete failure') }
      assert.equal((await executeJob(job(later, 'legacy-ack-failed'), 'jobs', qrl, opt)).jobsFailed, 1)
      assert.ok(await redis.exists(cacheKey))
      deleteSingle = async () => ({})
      assert.equal((await executeJob(job(later, 'legacy-ack-success'), 'jobs', qrl, opt)).jobsSucceeded, 1)
      assert.equal(await redis.exists(cacheKey), 0)
    })
  }
} finally {
  if (redis.status === 'ready') {
    await resetCache().catch(() => {})
    redis.disconnect()
  }
  if (server.exitCode === null && server.signalCode === null && server.pid) server.kill('SIGTERM')
  await finished
  await rm(scratch, { recursive: true })
}
console.log(JSON.stringify({ format, mode, cases, redisCollected: true }))
