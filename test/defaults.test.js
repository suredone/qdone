import {
  validateInteger,
  validateMessageOptions,
  getOptionsWithDefaults,
  setupAWS,
  setupVerbose
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
})

describe('getOptionsWithDefaults', () => {
  test('undefined args and blank object return same thing', () => {
    expect(getOptionsWithDefaults()).toEqual(getOptionsWithDefaults({}))
  })
  test('blank prefix overrides default prefix', () => {
    expect(getOptionsWithDefaults({ prefix: '' })).toEqual(getOptionsWithDefaults({ prefix: '' }))
  })
  test('can trun dlq off through options', () => {
    expect(getOptionsWithDefaults({ dlq: true })).toEqual(getOptionsWithDefaults({ dlq: true }))
    expect(getOptionsWithDefaults({ dlq: false })).toEqual(getOptionsWithDefaults({ dlq: false }))
    expect(getOptionsWithDefaults({ dlq: 1 })).toEqual(getOptionsWithDefaults({ dlq: 1 }))
    expect(getOptionsWithDefaults({ dlq: 0, dlqName: 0 })).toEqual(getOptionsWithDefaults({ dlq: 0 }))
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

describe('setupAWS', () => {
  test('AWS_REGION should be set if options include region', () => {
    const region = 'us-west-1'
    setupAWS({ region })
    expect(process.env.AWS_REGION).toEqual(region)
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
