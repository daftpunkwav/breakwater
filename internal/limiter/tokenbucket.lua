-- Atomic RPM/TPM token bucket check.
--
-- KEYS[1] = rpm bucket hash, KEYS[2] = tpm bucket hash
-- ARGV[1] = rpm capacity, ARGV[2] = tpm capacity, ARGV[3] = requested
-- tokens
--
-- Returns {allowed(0|1), retry_after_ms}.
-- The clock is Redis TIME, never the application clock. Buckets start
-- full; refill is continuous from the stored timestamp; capacity 0
-- disables the dimension. The whole refill-check-deduct sequence is
-- atomic per invocation.

local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local rpm_cap = tonumber(ARGV[1])
local tpm_cap = tonumber(ARGV[2])
local want = tonumber(ARGV[3])

local function refill(key, cap)
    if cap <= 0 then
        return -1
    end
    local tokens = tonumber(redis.call('HGET', key, 'tokens'))
    local ts = tonumber(redis.call('HGET', key, 'ts'))
    if tokens == nil then
        redis.call('HSET', key, 'tokens', tostring(cap), 'ts', now)
        redis.call('PEXPIRE', key, 120000)
        return cap
    end
    local rate = cap / 60000
    tokens = math.min(cap, tokens + (now - ts) * rate)
    redis.call('HSET', key, 'tokens', tostring(tokens), 'ts', now)
    redis.call('PEXPIRE', key, 120000)
    return tokens
end

local rpm = refill(KEYS[1], rpm_cap)
local tpm = refill(KEYS[2], tpm_cap)

local rpm_ok = rpm == -1 or rpm >= 1
local tpm_ok = tpm == -1 or tpm >= want

if rpm_ok and tpm_ok then
    if rpm ~= -1 then
        redis.call('HSET', KEYS[1], 'tokens', tostring(rpm - 1))
    end
    if tpm ~= -1 then
        redis.call('HSET', KEYS[2], 'tokens', tostring(tpm - want))
    end
    return {1, 0}
end

local wait = 0
if rpm ~= -1 and not rpm_ok then
    wait = math.max(wait, (1 - rpm) / (rpm_cap / 60000) * 1000)
end
if tpm ~= -1 and not tpm_ok then
    wait = math.max(wait, (want - tpm) / (tpm_cap / 60000) * 1000)
end
if wait < 1 then
    wait = 1
end
return {0, math.ceil(wait)}
