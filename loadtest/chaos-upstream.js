// chaos-upstream: the mock upstream fails ~100%; asserts the breaker
// opens (failures become fast 503s instead of slow timeouts) and later
// recovers through a half-open probe (invariants I4/I6).
//
// Start the stack with a fully failing upstream:
//   go run ./cmd/mockllm -addr 127.0.0.1:8090 -error-rate 1.0
// Run:
//   k6 run -e BASE_URL=... -e API_KEY=... chaos-upstream.js
// Mid-run, restart mockllm healthy (-error-rate 0) to watch recovery.
import http from 'k6/http';
import { check } from 'k6';
import { Trend } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1:8080';
const API_KEY = __ENV.API_KEY || 'bw-local-t1';

const failureLatency = new Trend('failure_latency');

export const options = {
  scenarios: {
    chaos: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 50),
      timeUnit: '1s',
      duration: __ENV.DURATION || '120s',
      preAllocatedVUs: 50,
      maxVUs: 200,
    },
  },
  thresholds: {
    // The whole point: once the breaker is open, failures are FAST.
    failure_latency: ['p(99)<100'],
  },
};

const body = JSON.stringify({
  model: 'mock-gpt',
  temperature: 0.7,
  messages: [{ role: 'user', content: 'chaos' }],
});

export default function () {
  const res = http.post(`${BASE_URL}/v1/chat/completions`, body, {
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${API_KEY}` },
    timeout: '70s',
  });
  failureLatency.add(res.timings.duration);
  check(res, {
    'served or fast-failed': (r) => [200, 502, 503].includes(r.status),
  });
}
// The breaker timeline (open -> half-open -> closed) is read from:
//   curl -s $BASE_URL/metrics | grep -E 'circuit_(open|half_open|state)'
