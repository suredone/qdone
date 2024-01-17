/**
 * Component to track event loop latency, which can be used as a metric for
 * backpressure.
 */

export class SystemMonitor {
  constructor (reportCallback, reportSeconds = 1) {
    this.reportCallback = reportCallback || console.log
    this.reportSeconds = reportSeconds
    this.measurements = []
    this.measure()
    this.reportLatency()
  }

  measure () {
    clearTimeout(this.measureTimeout)
    const start = new Date()
    this.measureTimeout = setTimeout(() => {
      const latency = new Date() - start
      this.measurements.push(latency)
      if (this.measurements.length > 1000) this.measurements.shift()
      this.measure()
    })
  }

  getLatency () {
    return this.measurements.length ? this.measurements.reduce((a, b) => a + b, 0) / this.measurements.length : 0
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

  shutdown () {
    clearTimeout(this.measureTimeout)
    clearTimeout(this.reportTimeout)
  }
}
