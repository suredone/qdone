/**
 * Exponential backoff controller.
 * usage:
 *   const exp = new ExponentialBackoff()
 *   const result = await exp.run(
 *     function action (attemptNumber) {
 *        console.log(attemptNumber) // 1, 2, 3, ...
 *        return axios.post(...)
 *     },
 *     function shouldRetry (returnValue, error) {
 *       if (returnValue && return value.code = 500) return true
 *       if (error && error.message === 'Internal Server Error') return true
 *     }
 *   )
 */

export class ExponentialBackoff {
  /**
   * Creates various behaviors for backoff.
   * @param {number} maxRetries - Number of times to attempt the action before
   *  throwing an error. Defaults to 3.
   * @param {number} maxJitterPercent - Jitter as a percentage of the delay.
   *  For example, if the exponential delay is 2 seconds, then a jitter of
   *  0.5 could lead to a delay as low as 1 second and as high as 3 seconds,
   *  since 0.5 * 2 = 1. Defaults to 0.5.
   * @param {number} exponentBase - The base for the exponent. Defaults to 2,
   *  which means the delay doubles every attempt.
   */
  constructor (maxRetries = 3, maxJitterPercent = 0.5, exponentBase = 2) {
    if (maxRetries < 1) throw new Error('maxRetries must be >= 1')
    if (maxJitterPercent < 0.1 || maxJitterPercent > 1) throw new Error('maxJitterPercent must be in the interval [0.1, 1]')
    if (exponentBase < 1 || exponentBase > 10) throw new Error('exponentBase must be in the range [1, 10]')
    this.maxRetries = parseInt(maxRetries)
    this.maxJitterPercent = parseFloat(maxJitterPercent)
    this.exponentBase = parseFloat(exponentBase)
    this.attemptNumber = 0
  }

  /**
   * Calculates how many ms to delay based on the current attempt number.
   */
  calculateDelayMs (attemptNumber) {
    const secondsRaw = this.exponentBase ** attemptNumber // 2, 4, 8, 16, ....
    const jitter = this.maxJitterPercent * (Math.random() - 0.5) // [-0.5, 0.5]
    const delayMs = Math.round(secondsRaw * (1 + jitter) * 1000)
    // console.log({ secondsRaw, jitter, delayMs })
    return delayMs
  }

  /**
   * Resolves after a delay set by the current attempt.
   */
  async delay (attemptNumber) {
    // console.log(attemptNumber)
    const delay = this.calculateDelayMs(attemptNumber)
    // console.log({ function: 'delay', attemptNumber, delay })
    return new Promise((resolve, reject) => setTimeout(resolve, delay))
  }

  /**
   * Call another function repeatedly, retrying with exponential backoff and
   * jitter if not successful.
   * @param {ExponentialBackoff~action} action - Callback that does the action
   *  to be attempted (web request, rpc, database call, etc). Will be called
   *  again after the exponential dealy if shouldRetry() returns true.
   * @param {ExponentialBackoff~shouldRetry} shouldRetry - Callback that gets
   *  to look at the return value of action() and any potential exception. If
   *  this returns true then the action will be retried with the appropriate
   *  backoff delay. Defaults to a function that returns true if an exception
   *  is thrown.
   */
  async run (
    action = async (attemptNumber) => undefined,
    shouldRetry = async (returnValue, error) => !!error
  ) {
    let attemptNumber = 0
    while (attemptNumber++ < this.maxRetries) {
      try {
        const result = await action(attemptNumber)
        if (await shouldRetry(result, undefined)) {
          if (attemptNumber >= this.maxRetries) throw new Error('Maximum number of attempts reached')
          await this.delay(attemptNumber)
        } else {
          return result
        }
      } catch (e) {
        if (await shouldRetry(undefined, e)) {
          if (attemptNumber >= this.maxRetries) throw e
          await this.delay(attemptNumber)
        } else {
          throw e
        }
      }
    }
  }

  /**
   * Callback used by run().
   * @callback ExponentialBackoff~action
   * @param {number} attemptNumber - Which attempt this is, i.e. 1, 2, 3, ...
   */

  /**
   * Callback used by run().
   * @callback ExponentialBackoff~shouldRetry
   * @param returnValue - The value returned by your action. If an exception
   *  was thrown by the action then this is undefined.
   * @param error - The exception thrown by your action. If there was no
   *  exception, this is undefined.
   */
}
