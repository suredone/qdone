/**
 * Command allowlist policy for the qdone worker.
 *
 * Tokenizes a job body with shell-quote and rejects anything that isn't a flat
 * list of literal string arguments. shell-quote represents operators, globs,
 * comments, and substitutions as OBJECTS, so "every token is a string" means the
 * body is a plain `binary arg arg` invocation with no shell-active constructs.
 * Validated commands are then matched against a JSON allowlist by binary + script
 * / subcommand, and (in enforce mode) executed via execFile with no shell.
 */
import { readFileSync, statSync } from 'fs'
import path from 'path'
import { parse as shellParse } from 'shell-quote'

export class CommandPolicyError extends Error {}

const allowlistCache = new Map() // path -> { mtimeMs, data }

export function _resetAllowlistCache () { allowlistCache.clear() }

/**
 * Tokenize a job body into a flat argv of literal strings, or throw if it
 * contains any shell-active construct (operator, glob, comment, substitution).
 */
export function parseCommandBody (body) {
  if (typeof body !== 'string' || body.trim() === '') {
    throw new CommandPolicyError('empty command body')
  }
  // shell-quote treats backticks as literal text, but /bin/sh performs command
  // substitution on them. Reject outright so the parser's verdict is sound under
  // both the audit (shell) and enforce (execFile) execution paths.
  if (body.includes('`')) {
    throw new CommandPolicyError('shell construct not allowed in command: backtick')
  }
  const tokens = shellParse(body)
  if (tokens.length === 0) throw new CommandPolicyError('command parsed to zero tokens')
  for (const t of tokens) {
    if (typeof t !== 'string') {
      const kind = (t && (t.op || (t.comment !== undefined ? 'comment' : (t.pattern !== undefined ? 'glob' : 'operator')))) || 'operator'
      throw new CommandPolicyError(`shell construct not allowed in command: ${kind}`)
    }
  }
  return tokens
}

export function loadAllowlist (filePath) {
  if (!filePath) throw new CommandPolicyError('no command allowlist file configured')
  const raw = readFileSync(filePath, 'utf8') // throws if missing/unreadable
  let data
  try {
    data = JSON.parse(raw)
  } catch (err) {
    throw new CommandPolicyError(`allowlist file is not valid JSON: ${err.message}`)
  }
  if (!data || typeof data.binaries !== 'object' || data.binaries === null) {
    throw new CommandPolicyError('allowlist file missing "binaries" object')
  }
  return data
}

function loadAllowlistCachedWithMtime (filePath) {
  if (!filePath) throw new CommandPolicyError('no command allowlist file configured')
  const mtimeMs = statSync(filePath).mtimeMs // throws if missing/unreadable
  const cached = allowlistCache.get(filePath)
  if (cached && cached.mtimeMs === mtimeMs) return cached.data
  const data = loadAllowlist(filePath)
  allowlistCache.set(filePath, { mtimeMs, data })
  return data
}

/**
 * Validate a parsed argv against an allowlist. Returns { ok, reason }.
 * Each allowlist binary entry is one of three matcher shapes:
 *   { scriptDirs:[...], scripts:[...] }   e.g. /usr/bin/php <dir>/<script>.php ...
 *   { subcommands:[...] }                 e.g. /usr/bin/sd <Subcommand> ...
 *   { fixedPrefix:[...], commands:[...] } e.g. /usr/bin/npm --prefix X run command <name> ...
 */
export function validateArgv (argv, allowlist) {
  const bin = argv[0]
  const entry = allowlist.binaries[bin]
  if (!entry) return { ok: false, reason: `binary not allowlisted: ${bin}` }

  if (entry.scripts) {
    const script = argv[1]
    if (!script) return { ok: false, reason: `missing script argument for ${bin}` }
    const dir = path.dirname(script) + '/'
    const base = path.basename(script)
    if (!(entry.scriptDirs || []).includes(dir)) return { ok: false, reason: `script dir not allowed: ${dir}` }
    if (!entry.scripts.includes(base)) return { ok: false, reason: `script not allowlisted: ${base}` }
    return { ok: true }
  }
  if (entry.subcommands) {
    const sub = argv[1]
    if (!entry.subcommands.includes(sub)) return { ok: false, reason: `subcommand not allowlisted: ${bin} ${sub}` }
    return { ok: true }
  }
  if (entry.fixedPrefix) {
    for (let i = 0; i < entry.fixedPrefix.length; i++) {
      if (argv[1 + i] !== entry.fixedPrefix[i]) return { ok: false, reason: `fixed-prefix mismatch at arg ${1 + i}` }
    }
    const sub = argv[1 + entry.fixedPrefix.length]
    if (!(entry.commands || []).includes(sub)) return { ok: false, reason: `command not allowlisted: ${sub}` }
    return { ok: true }
  }
  return { ok: false, reason: `allowlist entry for ${bin} has no matcher` }
}

/**
 * Orchestrator used by the worker. Returns one of:
 *   { ok: true, argv }              valid — run via execFile(argv) in enforce mode
 *   { ok: false, reason }           violation — reject (enforce) or log + shell-exec (audit)
 *   { ok: true, argv: null, misconfig } config error — fail open to shell-exec + alert
 */
export function checkCommand (body, opt) {
  let allowlist
  try {
    allowlist = loadAllowlistCachedWithMtime(opt.commandAllowlistFile)
  } catch (err) {
    return { ok: true, argv: null, misconfig: err.message }
  }
  let argv
  try {
    argv = parseCommandBody(body)
  } catch (err) {
    return { ok: false, reason: err.message }
  }
  const { ok, reason } = validateArgv(argv, allowlist)
  return ok ? { ok: true, argv } : { ok: false, reason }
}
