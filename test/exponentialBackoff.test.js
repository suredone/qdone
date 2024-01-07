import { jest } from '@jest/globals'
import { ExponentialBackoff } from '../src/exponentialBackoff.js'

describe('ExpontentialBackoff', () => {

  beforeAll(() => {
    jest.useFakeTimers()
    jest.spyOn(global, 'setTimeout')
  })

  test('should retry after one failure', async () => {
    const exp = new ExponentialBackoff()
    const action = jest.fn(
      async (attemptNumber) => {
        // console.log({ attemptNumber })
        if (attemptNumber === 1) throw new Error('this one failed')
        else return attemptNumber
      }
    )
    const shouldRetry = jest.fn(
      async (returnValue, error) => {
        // console.log({ returnValue, error })
        if (error) return true
        else return false
      }
    )
    // const result = await exp.run(action, shouldRetry)
    const resultPromise = exp.run(action, shouldRetry)
    expect(action).toHaveBeenCalledTimes(1)
    expect(shouldRetry).toHaveBeenCalledTimes(0)

    // After action attempt, we await shouldRetry()
    await Promise.resolve()
    expect(action).toHaveBeenCalledTimes(1)
    expect(shouldRetry).toHaveBeenCalledTimes(1)

    // await this.delay(...)
    jest.runAllTimers()
    await Promise.resolve()

    jest.runAllTimers()
    const result = await resultPromise
    expect(action).toHaveBeenCalledTimes(2)
    expect(shouldRetry).toHaveBeenCalledTimes(2)
    expect(result).toBe(2)
  })

  test('should retry after two failures', async () => {
    const exp = new ExponentialBackoff()
    const action = jest.fn(
      async (attemptNumber) => {
        // console.log({ function: 'attempt', attemptNumber })
        if (attemptNumber === 1 || attemptNumber === 2) throw new Error('this one failed')
        else return attemptNumber
      }
    )
    const shouldRetry = jest.fn(
      async (returnValue, error) => {
        // console.log({ function: 'shouldRetry', returnValue, error })
        if (error) return true
        else return false
      }
    )
    // const result = await exp.run(action, shouldRetry)
    const resultPromise = exp.run(action, shouldRetry)
    expect(action).toHaveBeenCalledTimes(1)
    expect(shouldRetry).toHaveBeenCalledTimes(0)
    // console.log('one')

    // After action attempt, we await shouldRetry()
    await Promise.resolve()
    expect(action).toHaveBeenCalledTimes(1)
    expect(shouldRetry).toHaveBeenCalledTimes(1)
    // console.log('two')

    // await this.delay(attemptNumber)
    await Promise.resolve()
    // delay -> setTimeout
    jest.runAllTimers()
    // console.log('three')

    jest.runAllTimers()
    await Promise.resolve()
    jest.runAllTimers()
    // console.log('four')

    jest.runAllTimers()
    await Promise.resolve()
    jest.runAllTimers()
    // console.log('five')

    jest.runAllTimers()
    await Promise.resolve()
    jest.runAllTimers()
    await Promise.resolve()
    jest.runAllTimers()
    await Promise.resolve()
    jest.runAllTimers()
    await Promise.resolve()
    expect(action).toHaveBeenCalledTimes(2)
    expect(shouldRetry).toHaveBeenCalledTimes(2)
    // console.log('six')

    const result = await resultPromise
    expect(action).toHaveBeenCalledTimes(3)
    expect(shouldRetry).toHaveBeenCalledTimes(3)
    expect(result).toBe(3)
  })
})
