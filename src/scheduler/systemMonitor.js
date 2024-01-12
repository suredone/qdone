/**
 * Component to track event loop latency, which can be used as a metric for
 * backpressure.
 */

export class SystemMonitor {
  constructor (opt, smoothingFactor = 0.5, reportSeconds = 5) {
    this.opt = opt
    this.smoothingFactor = smoothingFactor
    this.reportSeconds = reportSeconds
    this.measurements = {
      setTimeout: [],
      setImmediate: []
    }
    this.timeouts = {
      setTimeout: undefined,
      setImmediate: undefined,
      reportLatency: undefined
    }
    this.measureLatencySetTimeout()
    this.reportLatency()
  }

  measureLatencySetTimeout () {
    const start = new Date()
    this.timeouts.setTimeout = setTimeout(() => {
      const latency = new Date() - start
      this.measurements.setTimeout.push(latency)
      if (this.measurements.setTimeout.length > 1000) this.measurements.setTimeout.shift()
      this.measureLatencySetTimeout()
    })
  }

  getLatency () {
    const results = {}
    for (const k in this.measurements) {
      const values = this.measurements[k]
      results[k] = values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0
    }
    return results
  }

  reportLatency () {
    this.timeouts.reportLatency = setTimeout(() => {
      for (const k in this.measurements) {
        const values = this.measurements[k]
        const mean = values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0
        console.log({ [k]: mean })
      }
      this.reportLatency()
    }, this.reportSeconds * 1000)
  }

  shutdown () {
    console.log(this.measurements)
    for (const k in this.timeouts) clearTimeout(this.timeouts[k])
  }
}

