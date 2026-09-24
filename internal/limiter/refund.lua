-- Refund unconsumed tokens into the TPM bucket.
--
-- KEYS[1] = tpm bucket hash
-- ARGV[1] = tpm capacity, ARGV[2] = tokens to return
--
-- The refund raises the bucket toward capacity after a refill to now
-- (Redis TIME); it can never exceed capacity. A missing bucket is
-- treated as a full one: refunding into a full bucket is a no-op.

local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local cap = tonumber(ARGV[1])
local refund = tonumber(ARGV[2])

if cap <= 0 or refund <= 0 then
    return 0
end

local tokens = tonumber(redis.call('HGET', KEYS[1], 'tokens'))
local ts = tonumber(redis.call('HGET', KEYS[1], 'ts'))
if tokens == nil then
    tokens = cap
    ts = now
end

local rate = cap / 60000
tokens = math.min(cap, tokens + (now - ts) * rate + refund)
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', now)
redis.call('PEXPIRE', KEYS[1], 120000)
return 1
