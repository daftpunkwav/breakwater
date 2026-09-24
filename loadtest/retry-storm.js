// retry-storm: an intermittently failing upstream (50% injected errors)
// plus a small per-request attempt cap; asserts the retry amplification
// stays bounded by the budgets (invariant I5).
//
//   go run ./cmd/mockllm -addr 127.0.0.1:8090 -error-rate 0.5
//   k6 run -e BASE_URL=... -e API_KEY=... retry-storm.js
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1:8080';
const API_KEY = __ENV.API_KEY || 'bw-local-t1';

export const options = {
  scenarios: {
    storm: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 100),
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: 100,
      maxVUs: 400,
    },
  },
};

const body = JSON.stringify({
  model: 'mock-gpt',
  temperature: 0.7,
  messages: [{ role: 'user', content: 'storm' }],
});

export default function () {
  const res = http.post(`${BASE_URL}/v1/chat/completions`, body, {
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${API_KEY}` },
    timeout: '70s',
  });
  check(res, { 'a response arrived': (r) => r.status > 0 });
}
// Amplification is read after the run:
//   curl -s $BASE_URL/metrics | grep -E 'retry_attempts_total|requests_total'
// Assertion: attempts beyond the first stay within
// MaxAttempts-1 per served request, and budget_exhausted_total shows
// the budget biting before the upstream melts.
