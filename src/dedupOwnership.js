import { randomBytes } from 'crypto'

const parse = `
local function ownerRecord(value)
  if not value then return end
  local token, count, state, messageId = string.match(value, '^owner%-v1:([0-9a-f]+):(%d+):([a-z]+):(.*)$')
  count = tonumber(count)
  if not token or #token ~= 32 or not count or count > 9007199254740990 then return end
  if (state == 'pending' and messageId == '') or
     (state == 'accepted' and string.find(messageId, '%S')) then
    return token, count, state, messageId
  end
end

local function numericRecord(value)
  return value and string.match(value, '^[1-9]%d*$')
end

local function ownerValue(token, count, state, messageId)
  return 'owner-v1:' .. token .. ':' .. count .. ':' .. state .. ':' .. messageId
end
`

const claim = parse + `
local key, protocol, token, seconds, stats = KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4]
local value = redis.call('GET', key)
if not value then
  if protocol == 'owner-v1' then
    redis.call('SET', key, ownerValue(token, 0, 'pending', ''), 'EX', seconds)
  else
    redis.call('SET', key, '1', 'EX', seconds)
  end
  return {'claimed'}
end
if numericRecord(value) then
  if protocol == 'owner-v1' then return {'unknown', 'legacy-owner'} end
  return {'legacyDeduplicated', redis.call('INCR', key) - 1}
end
local current, count, state, messageId = ownerRecord(value)
if not current then return {'unknown', 'invalid-owner'} end
local ttl = redis.call('PTTL', key)
if ttl < 0 then return {'unknown', 'invalid-owner'} end
if state == 'pending' then return {'unknown', 'pending-owner'} end
if stats == '1' and ttl > 0 then
  count = count + 1
  redis.call('SET', key, ownerValue(current, count, state, messageId), 'PX', ttl, 'XX')
end
return {'deduplicated', messageId, count, ttl}
`

const confirm = parse + `
local key, token, messageId = KEYS[1], ARGV[1], ARGV[2]
local current, count, state, previousId = ownerRecord(redis.call('GET', key))
if current ~= token then return 0 end
local ttl = redis.call('PTTL', key)
if ttl <= 0 then return 0 end
if state == 'accepted' and previousId == messageId then return 1 end
if state ~= 'pending' then return 0 end
if redis.call('SET', key, ownerValue(token, count, 'accepted', messageId), 'PX', ttl, 'XX') then
  return 1
end
return 0
`

const release = parse + `
local key, token, pendingOnly = KEYS[1], ARGV[1], ARGV[2]
local value = redis.call('GET', key)
if token == '' then
  if pendingOnly == '0' and numericRecord(value) then return redis.call('DEL', key) end
else
  local current, _, state = ownerRecord(value)
  if current == token and (pendingOnly == '0' or state == 'pending') then return redis.call('DEL', key) end
end
return 0
`

export async function claimOwner (client, key, opt) {
  const protocol = opt.dedupProtocol ?? 'legacy'
  const token = protocol === 'owner-v1' ? randomBytes(16).toString('hex') : ''
  const reply = await client.eval(claim, 1, key, protocol, token, opt.dedupPeriod, opt.dedupStats ? '1' : '0')
  return { reply, token }
}

export function confirmOwner (client, key, token, messageId) {
  return client.eval(confirm, 1, key, token, messageId)
}

export function releaseOwner (clientOrPipeline, key, token, pendingOnly) {
  return clientOrPipeline.eval(release, 1, key, token, pendingOnly ? '1' : '0')
}
