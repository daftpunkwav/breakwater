// rate-limit: an over-limit stampede must get 100% 429 with
// Retry-After, and the upstream QPS must show a hard ceiling
// (invariant I1: rejected requests never touch an upstream).
//
//   k6 run -e BASE_URL=... -e API_KEY=... rate-limit.js
// Tier RPM must be small for the run (e.g. 60): over-limit is the point.
import http from 'k6/http';
import { check } from 'k6';
import { Rate } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1:8080';
const API_KEY = __ENV.API_KEY || 'bw-local-t1';

export const options = {
  scenarios: {
    stampede: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 1000),
      timeUnit: '1s',
      duration: __ENV.DURATION || '30s',
      preAllocatedVUs: 200,
      maxVUs: 1000,
    },
  },
};

const limited = new Rate('limited_429');

const body = JSON.stringify({
  model: 'mock-gpt',
  temperature: 0.7,
  messages: [{ role: 'user', content: 'limit me' }],
});

export default function () {
  const res = http.post(`${BASE_URL}/v1/chat/completions`, body, {
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${API_KEY}` },
  });
  if (res.status === 429) {
    limited.add(1);
    check(res, { 'retry-after present': (r) => r.headers['Retry-After'] !== undefined });
  } else {
    limited.add(0);
    check(res, { 'allowed requests succeed': (r) => r.status === 200 });
  }
}
// Evidence: limited_429 rate matches (1 - tier RPM / offered rate), and
// mockllm's own request count confirms the hard ceiling.
