import { reportEvent } from '../src/sentry.js'

describe('reportEvent', () => {
  test('is a no-op (resolves) when sentryDsn is unset', async () => {
    await expect(reportEvent({}, 'error', 'msg', { a: { b: 1 } })).resolves.toBeUndefined()
  })
})
