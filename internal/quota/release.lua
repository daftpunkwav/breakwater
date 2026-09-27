-- Release one lease without consumption (cancel or sweeper expiry).
--
-- KEYS[1] = balance key, KEYS[2] = lease hash key, KEYS[3] = sweep zset,
-- KEYS[4] = refunded counter
-- ARGV[1] = lease id, ARGV[2] = target state ('CANCELLED' or 'EXPIRED'),
-- ARGV[3] = audit ttl_ms
--
-- Returns 1 when this call moved a RESERVED lease to the target state
-- and refunded its full amount; 0 otherwise (already terminal or
-- missing).
-- The state check and refund are one atomic step, so a sweeper and a
-- late settle can never both refund the same lease. The balance credit
-- and the refunded counter move together: the reconcile identity pairs
-- every balance movement with exactly one counter movement.

local state = redis.call('HGET', KEYS[2], 'state')
redis.call('ZREM', KEYS[3], ARGV[1])
if not state or state ~= 'RESERVED' then
    return 0
end

local amount = tonumber(redis.call('HGET', KEYS[2], 'amount'))
-- %d keeps the refund an integer string; the raw Lua double would
-- serialize as scientific notation past 14 digits and the INCRBY
-- would reject it.
redis.call('INCRBY', KEYS[1], string.format('%d', amount))
redis.call('HSET', KEYS[2], 'state', ARGV[2])
redis.call('INCRBY', KEYS[4], string.format('%d', amount))
-- The terminal record stays for a bounded audit window, then expires.
redis.call('PEXPIRE', KEYS[2], tonumber(ARGV[3]))
return 1
