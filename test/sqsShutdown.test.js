/* eslint-env jest */

import { jest } from '@jest/globals'
import { getSQSClient, getSQSMaintenanceClient, setSQSClient, shutdownSQSClient } from '../src/sqs.js'

describe('SQS clients', () => {
  test('the maintenance client defaults to the receive client and can be set apart', () => {
    const receive = { destroy: jest.fn() }
    setSQSClient(receive)
    expect(getSQSClient()).toBe(receive)
    expect(getSQSMaintenanceClient()).toBe(receive)

    const maintenance = { destroy: jest.fn() }
    setSQSClient(receive, maintenance)
    expect(getSQSMaintenanceClient()).toBe(maintenance)
    expect(getSQSClient()).toBe(receive)
  })

  test('shutdown destroys both clients once and refuses new ones afterwards', async () => {
    const receive = { destroy: jest.fn() }
    const maintenance = { destroy: jest.fn() }
    setSQSClient(receive, maintenance)
    await shutdownSQSClient()
    await shutdownSQSClient()
    expect(receive.destroy).toHaveBeenCalledTimes(1)
    expect(maintenance.destroy).toHaveBeenCalledTimes(1)
    expect(() => getSQSClient()).toThrow('qdone SQS client is shut down')
    expect(() => getSQSMaintenanceClient()).toThrow('qdone SQS client is shut down')
  })
})
