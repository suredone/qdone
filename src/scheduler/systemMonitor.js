/**
 * Component to track event loop latency, which can be used as a metric for
 * backpressure.
 */

import os from 'os'

export class SystemMonitor {
  constructor (reportCallback, reportSeconds = 1) {
    this.reportCallback = reportCallback || console.log
    this.reportSeconds = reportSeconds
    this.latencies = []
    this.oneMinuteLoad = os.loadavg()[0]
    this.instantaneousLoad = this.oneMinuteLoad
    this.measure()
    this.reportLatency()
  }

  measure () {
    clearTimeout(this.measureTimeout)
    const start = new Date()
    this.measureTimeout = setTimeout(() => {
      this.measureLatency(start)
      this.measureLoad()
      this.measure()
    })
  }

  measureLatency (start) {
    const latency = new Date() - start
    this.latencies.push(latency)
    if (this.latencies.length > 1000) this.latencies.shift()
  }

  getLatency () {
    return this.latencies.length ? this.latencies.reduce((a, b) => a + b, 0) / this.latencies.length : 0
  }

  reportLatency () {
    clearTimeout(this.reportTimeout)
    this.reportTimeout = setTimeout(() => {
      const latency = this.getLatency()
      // console.log({ latency })
      if (this.reportCallback) this.reportCallback(latency)
      this.reportLatency()
    }, this.reportSeconds * 1000)
  }

  /**
   * Measures load over the last five seconds instead of being averaged over one
   * minute. This lets the scheduler respond much faster to dips in load.
   *
   * Theory:
   *
   *  The Linux kernel calculates the moving average something like:
   *    A_1 = A_0 * e + A_now (1 - e)
   *  Where:
   *   - A_now is the number of processes active/waiting
   *   - A_1 is the new one-minute load average after the measurement of A_now
   *   - A_0 is the previous one-minute average
   *   - e is 1884/2048.
   *
   *  Solving this for A_now, which we want to access, we get:
   *   A_now = (A_1 - A_0 * e) / (1 - e)
   *
   *  We use this formula below to extract A_now when we detect a change in A_1.
   *
   * Note: this code assums that we are observing the average often enough to
   * detect each change. So you have to call it at least every 5 seconds. 1
   * second is better to reduce latency of detecting the change.
   */

  measureLoad () {
    const [newLoad] = os.loadavg()
    const previousLoad = this.oneMinuteLoad
    if (previousLoad !== newLoad) {
      const e = 1884 / 2048 // see include/linux/sched/loadavg.h
      const active = (newLoad - previousLoad * e) / (1 - e)
      // We take the min here so that spikes up in load are averaged out. We
      // care about detecting spikes downward so we can allow more jobs to run.
      this.instantaneousLoad = Math.min(active, newLoad)
      this.oneMinuteLoad = newLoad
      console.log({ newLoad, previousLoad, active, instantaneousLoad: this.instantaneousLoad, oneMinuteLoad: this.oneMinuteLoad })
    }
  }

  getLoad () {
    return this.instantaneousLoad
  }

  shutdown () {
    clearTimeout(this.measureTimeout)
    clearTimeout(this.reportTimeout)
  }
}
