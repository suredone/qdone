/* eslint-env jest */

import { QueueManager } from '../src/scheduler/queueManager.js'
import { getOptionsWithDefaults } from '../src/defaults.js'

const qrl = 'https://sqs/sdqd_test'

// Build the manager without its constructor: that starts queue resolution against SQS, and
// updateIcehouse only needs the options and the icehouse table.
function manager (options) {
  const qm = Object.create(QueueManager.prototype)
  qm.opt = getOptionsWithDefaults(options)
  qm.icehouse = {}
  return qm
}

describe('QueueManager.updateIcehouse', () => {
  test('an empty receive waits at least the minimum backoff without a cap', () => {
    const qm = manager({})
    qm.updateIcehouse(qrl, true)
    expect(qm.icehouse[qrl].secondsToWait).toBeGreaterThanOrEqual(10)
    // The first empty receive seeds the entry at one and then counts itself
    expect(qm.icehouse[qrl].numEmptyReceives).toBe(2)
  })
  test('emptyReceiveBackoffSeconds caps the wait after a successful empty receive', () => {
    const qm = manager({ emptyReceiveBackoffSeconds: 5 })
    for (let i = 0; i < 4; i++) qm.updateIcehouse(qrl, true)
    expect(qm.icehouse[qrl].secondsToWait).toBeLessThanOrEqual(5)
    expect(qm.icehouse[qrl].numEmptyReceives).toBe(5)
  })
  test('a receive error keeps the escalating backoff even with the cap set', () => {
    const qm = manager({ emptyReceiveBackoffSeconds: 5 })
    for (let i = 0; i < 4; i++) qm.updateIcehouse(qrl, true, true)
    expect(qm.icehouse[qrl].secondsToWait).toBeGreaterThanOrEqual(10)
  })
  test('a non-empty receive releases the queue from the icehouse', () => {
    const qm = manager({ emptyReceiveBackoffSeconds: 5 })
    qm.updateIcehouse(qrl, true)
    qm.updateIcehouse(qrl, false)
    expect(qm.icehouse[qrl]).toBeUndefined()
  })
})
