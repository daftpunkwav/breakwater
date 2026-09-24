-- Settle a live lease against actual usage.
--
-- KEYS[1] = balance key, KEYS[2] = lease hash key, KEYS[3] = sweep zset
-- ARGV[1] = used tokens, ARGV[2] = lease id, ARGV[3] = audit ttl_ms
--
-- Returns {settled(0|1), refund_or_state}.
-- Only a RESERVED lease settles: terminal leases are detectable no-ops
-- (late settles after a sweeper expiry are expected), refunding
-- (reserved - used) when positive and never surcharging.

local state = redis.call('HGET', KEYS[2], 'state')
if not state then
    return {0, 'missing'}
end
if state ~= 'RESERVED' then
    return {0, state}
end

local amount = tonumber(redis.call('HGET', KEYS[2], 'amount'))
local used = tonumber(ARGV[1])
local refund = amount - used
if refund > 0 then
    redis.call('INCRBY', KEYS[1], refund)
end
redis.call('HSET', KEYS[2], 'state', 'SETTLED')
-- The terminal record stays for a bounded audit window, then expires:
-- the hash set must not grow without bound.
redis.call('PEXPIRE', KEYS[2], tonumber(ARGV[3]))
redis.call('ZREM', KEYS[3], ARGV[2])
return {1, math.max(refund, 0)}
