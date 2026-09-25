-- Atomically reserve quota: deduct the balance and record the lease.
--
-- KEYS[1] = balance key, KEYS[2] = lease hash key, KEYS[3] = sweep zset,
-- KEYS[4] = debited counter
-- ARGV[1] = now_ms, ARGV[2] = amount, ARGV[3] = lease ttl_ms,
-- ARGV[4] = lease id, ARGV[5] = tenant id
--
-- Returns {ok(0|1), balance}.
-- A missing balance key means the tenant was never provisioned: the
-- reservation is denied, never created from thin air.
--
-- The balance deduction and the debited counter move in the same atomic
-- step: the reconcile identity (balance may only fall by debits minus
-- refunds) pairs every balance movement with exactly one counter
-- movement, so in-flight reservations never read as drift.

local bal = tonumber(redis.call('GET', KEYS[1]))
if bal == nil then
    return {0, -1}
end

local amount = tonumber(ARGV[2])
if bal < amount then
    return {0, bal}
end

bal = bal - amount
redis.call('SET', KEYS[1], bal)
redis.call('INCRBY', KEYS[4], string.format('%d', amount))
redis.call('HSET', KEYS[2],
    'tenant', ARGV[5],
    'amount', amount,
    'state', 'RESERVED',
    'created', ARGV[1])
redis.call('ZADD', KEYS[3], ARGV[1] + ARGV[3], ARGV[4])
return {1, bal}
