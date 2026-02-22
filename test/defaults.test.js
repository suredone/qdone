import { jest } from '@jest/globals'
import {
  validateInteger,
  validateMessageOptions,
  getOptionsWithDefaults,
  setupAWS,
  setupVerbose,
  defaults
} from '../src/defaults.js'

describe('validateInteger', () => {
  test('integers validate to themselves', () => {
    const opt = {
      one: 123,
      two: 0,
      three: -12
    }
    expect(validateInteger(opt, 'one')).toBe(opt.one)
    expect(validateInteger(opt, 'two')).toBe(opt.two)
    expect(validateInteger(opt, 'three')).toBe(opt.three)
  })

  test('non integers that are not parsable throw an error', () => {
    const opt = {
      one: 'hi',
      two: null,
      three: -NaN,
      four: undefined,
      five: {}
    }
    expect(() => validateInteger(opt, 'one')).toThrow('needs to be an integer')
    expect(() => validateInteger(opt, 'two')).toThrow('needs to be an integer')
    expect(() => validateInteger(opt, 'three')).toThrow('needs to be an integer')
    expect(() => validateInteger(opt, 'four')).toThrow('needs to be an integer')
    expect(() => validateInteger(opt, 'five')).toThrow('needs to be an integer')
  })
})

describe('validateMessageOptions', () => {
  test('non objects are ignored and returned as an empty object', () => {
    expect(validateMessageOptions('one')).toEqual({})
    expect(validateMessageOptions()).toEqual({})
    expect(validateMessageOptions(null)).toEqual({})
    expect(validateMessageOptions(NaN)).toEqual({})
    expect(validateMessageOptions([])).toEqual({})
  })
  test('an error is thrown for the first invalid key', () => {
    expect(() => validateMessageOptions({ foo: 1 })).toThrow('Invalid message option foo')
    expect(() => validateMessageOptions({ bar: 1, foo: 1 })).toThrow('Invalid message option bar')
    expect(() => validateMessageOptions({ groupId: 1, foo: 1 })).toThrow('Invalid message option foo')
    expect(() => validateMessageOptions({ deduplicationId: 1, foo: 1 })).toThrow('Invalid message option foo')
  })
  test('options with valid keys are passed through', () => {
    expect(validateMessageOptions({ groupId: 1 })).toEqual({ groupId: 1 })
    expect(validateMessageOptions({ deduplicationId: 1 })).toEqual({ deduplicationId: 1 })
    expect(validateMessageOptions({ groupId: 1, deduplicationId: 1 })).toEqual({ groupId: 1, deduplicationId: 1 })
  })
  test('delay is a valid message option', () => {
    expect(validateMessageOptions({ delay: 15 })).toEqual({ delay: 15 })
    expect(validateMessageOptions({ delay: 0 })).toEqual({ delay: 0 })
    expect(validateMessageOptions({ delay: 15, groupId: 'g1' })).toEqual({ delay: 15, groupId: 'g1' })
    expect(validateMessageOptions({ delay: 15, groupId: 'g1', deduplicationId: 'd1' })).toEqual({ delay: 15, groupId: 'g1', deduplicationId: 'd1' })
  })
})

describe('getOptionsWithDefaults', () => {
  test('undefined args and blank object return same thing', () => {
    expect(getOptionsWithDefaults()).toEqual(getOptionsWithDefaults({}))
  })
  test('blank prefix overrides default prefix', () => {
    expect(getOptionsWithDefaults({ prefix: '' })).toEqual(getOptionsWithDefaults({ prefix: '' }))
  })
  test('dlq defaults to true when not specified', () => {
    expect(getOptionsWithDefaults().dlq).toBe(true)
    expect(getOptionsWithDefaults({}).dlq).toBe(true)
  })
  test('dlq can be explicitly enabled', () => {
    expect(getOptionsWithDefaults({ dlq: true }).dlq).toBe(true)
  })
  test('dlq can be explicitly disabled', () => {
    expect(getOptionsWithDefaults({ dlq: false }).dlq).toBe(false)
  })
  test('dlq enabled when dlq sub-options are set', () => {
    expect(getOptionsWithDefaults({ dlqSuffix: '_custom_dead' }).dlq).toBe(true)
    expect(getOptionsWithDefaults({ dlqAfter: 5 }).dlq).toBe(true)
    expect(getOptionsWithDefaults({ 'dlq-suffix': '_custom_dead' }).dlq).toBe(true)
  })
  test('dlq falsy values disable dlq', () => {
    expect(getOptionsWithDefaults({ dlq: 0 }).dlq).toBeFalsy()
    expect(getOptionsWithDefaults({ dlq: 0, dlqName: 0 }).dlq).toEqual(getOptionsWithDefaults({ dlq: 0 }).dlq)
  })
  test('externalDedup requires a cacheUri option', () => {
    expect(() => getOptionsWithDefaults({ externalDedup: true })).toThrow('requires the')
  })
  test('externalDedup requires a valid dedupPeriod', () => {
    expect(() => getOptionsWithDefaults({ externalDedup: true, cacheUri: 'foo', dedupPeriod: 0.1 })).toThrow('requires a')
  })
  test('deduplicationId and dedupIdPerMessage are mutually exclusive', () => {
    expect(() => getOptionsWithDefaults({ dedupIdPerMessage: true, deduplicationId: 'asdf' })).toThrow('Use either')
  })
})

describe('setupVerbose', () => {
  test('if stderr is a TTY then default to verbose', () => {
    const originalIsTTY = process.stderr.isTTY
    const options = {}
    process.stderr.isTTY = true
    setupVerbose(options)
    expect(options).toEqual({ verbose: true, quiet: false })
    process.stderr.isTTY = originalIsTTY
  })
  test('if stderr is not a TTY then default to quiet', () => {
    const originalIsTTY = process.stderr.isTTY
    const options = {}
    process.stderr.isTTY = false
    setupVerbose(options)
    expect(options).toEqual({ verbose: false, quiet: true })
    process.stderr.isTTY = originalIsTTY
  })
})

describe('environment variables', () => {
  const originalEnv = process.env

  beforeEach(() => {
    jest.resetModules()
    process.env = { ...originalEnv }
  })

  afterEach(() => {
    process.env = originalEnv
  })

  function testString (envName, apiName, val1, val2) {
    test(envName, () => {
      expect(getOptionsWithDefaults()[apiName]).toBe(defaults[apiName])
      process.env[envName] = val1
      expect(getOptionsWithDefaults({})[apiName]).toBe(val1)
      expect(getOptionsWithDefaults({ [apiName]: val2 })[apiName]).toBe(val2)
    })
  }

  function testBoolean (envName, apiName) {
    test(envName, () => {
      expect(getOptionsWithDefaults()[apiName]).toBe(false)
      process.env[envName] = true
      expect(getOptionsWithDefaults()[apiName]).toBe(false)
      process.env[envName] = 'true'
      expect(getOptionsWithDefaults()[apiName]).toBe(true)
    })
  }

  function testInteger (envName, apiName, val1, val2) {
    test(envName, () => {
      expect(getOptionsWithDefaults()[apiName]).toBe(defaults[apiName])
      process.env[envName] = val1
      expect(getOptionsWithDefaults()[apiName]).toBe(val1)
      expect(getOptionsWithDefaults({ [apiName]: val2 })[apiName]).toBe(val2)
      process.env[envName] = 'invalid integer'
      expect(() => getOptionsWithDefaults()).toThrow('integer')
    })
  }

  testString('QDONE_PREFIX', 'prefix', 'qdone_custom_', 'custom_')
  testString('QDONE_FAIL_SUFFIX', 'failSuffix', '_fail_custom', '_custom')

  test('QDONE_REGION', () => {
    expect(getOptionsWithDefaults().region).toBe('us-east-1')
    process.env.AWS_REGION = 'us-east-2'
    expect(getOptionsWithDefaults().region).toBe('us-east-2')
    process.env.QDONE_REGION = 'us-west-1'
    expect(getOptionsWithDefaults().region).toBe('us-west-1')
    expect(getOptionsWithDefaults({ region: 'ap-southeast-1' }).region).toBe('ap-southeast-1')
    expect(process.env.AWS_REGION).toBe('ap-southeast-1')
  })

  test('QDONE_QUIET / QDONE_VERBOSE', () => {
    expect(getOptionsWithDefaults().quiet).toBe(false)
    expect(getOptionsWithDefaults().verbose).toBe(false)
    expect(getOptionsWithDefaults({ verbose: true }).verbose).toBe(true)
    expect(getOptionsWithDefaults({ verbose: true }).quiet).toBe(false)
    expect(getOptionsWithDefaults({ quiet: true }).verbose).toBe(false)
    expect(getOptionsWithDefaults({ quiet: true }).quiet).toBe(true)
    process.env.QDONE_VERBOSE = true
    expect(getOptionsWithDefaults().verbose).toBe(false)
    process.env.QDONE_VERBOSE = 'true'
    expect(getOptionsWithDefaults().verbose).toBe(true)
    expect(getOptionsWithDefaults().quiet).toBe(false)
    process.env = originalEnv
    process.env.QDONE_QUIET = 'true'
    expect(getOptionsWithDefaults().quiet).toBe(true)
    expect(getOptionsWithDefaults().verbose).toBe(false)
  })

  testBoolean('QDONE_FIFO', 'fifo')
  testString('QDONE_SENTRY_DSN', 'sentryDsn', 'https://sentry.io/...', 'https://sentry.io/custom/...')
  testBoolean('QDONE_DISABLE_LOG', 'disableLog')
  testBoolean('QDONE_INCLUDE_FAILED', 'includeFailed')
  testBoolean('QDONE_INCLUDE_DEAD', 'includeDead')

  test('QDONE_EXTERNAL_DEDUP', () => {
    expect(getOptionsWithDefaults().externalDedup).toBe(false)
    process.env.QDONE_EXTERNAL_DEDUP = true
    expect(getOptionsWithDefaults().externalDedup).toBe(false)
    process.env.QDONE_EXTERNAL_DEDUP = 'true'
    process.env.QDONE_CACHE_URI = 'redis://...'
    expect(getOptionsWithDefaults().externalDedup).toBe(true)
  })

  testInteger('QDONE_DEDUP_PERIOD', 'dedupPeriod', 60 * 10, 60 * 20)
  testBoolean('QDONE_DEDUP_STATS', 'dedupStats')
  testString('QDONE_GROUP_ID', 'groupId', 'abcd123', 'xyz456')
  testString('QDONE_DEDUPLICATION_ID', 'deduplicationId', 'abcd123', 'xyz456')
  testBoolean('QDONE_DEDUP_ID_PER_MESSAGE', 'dedupIdPerMessage')
  testInteger('QDONE_MESSAGE_RETENTION_PERIOD', 'messageRetentionPeriod', 1000, 2000)
  testInteger('QDONE_DELAY', 'delay', 1000, 2000)
  testInteger('QDONE_SEND_RETRIES', 'sendRetries', 10, 20)
  testInteger('QDONE_FAIL_DELAY', 'failDelay', 1000, 2000)
  testString('QDONE_DLQ_SUFFIX', 'dlqSuffix', '_custom', '_custom_too')
  testInteger('QDONE_DLQ_AFTER', 'dlqAfter', 10, 20)
  testInteger('QDONE_WAIT_TIME', 'waitTime', 10, 15)
  testInteger('QDONE_KILL_AFTER', 'killAfter', 120, 300)
  testBoolean('QDONE_ARCHIVE', 'archive')
  testBoolean('QDONE_ACTIVE_ONLY', 'activeOnly')
  testInteger('QDONE_MAX_CONCURRENT_JOBS', 'maxConcurrentJobs', 1000, 2000)
  testInteger('QDONE_MAX_MEMORY_PERCENT', 'maxMemoryPercent', 80, 50)
  testInteger('QDONE_IDLE_FOR', 'idleFor', 120, 420)
  testBoolean('QDONE_DELETE', 'delete')
  testBoolean('QDONE_CREATE', 'create')
  testBoolean('QDONE_OVERWRITE', 'overwrite')
})

describe('setupAWS', () => {
  test('AWS_REGION should be set if options include region', () => {
    const region = 'us-west-1'
    setupAWS({ region })
    expect(process.env.AWS_REGION).toEqual(region)
  })
})
