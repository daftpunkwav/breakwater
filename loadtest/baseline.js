// baseline: pure forwarding throughput and latency, the reference the
// governance overhead is measured against (gateway self-loss).
//
// Needs: gateway with governance armed, mockllm upstream.
//   k6 run -e BASE_URL=http://127.0.0.1:8080 -e API_KEY=... baseline.js
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1:8080';
const API_KEY = __ENV.API_KEY || 'bw-local-t1';

export const options = {
  scenarios: {
    steady: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 200),
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: 100,
      maxVUs: 500,
    },
  },
};

const body = JSON.stringify({
  model: 'mock-gpt',
  temperature: 0.7, // cache-ineligible: baseline measures the forwarding path
  messages: [{ role: 'user', content: 'ping' }],
});

export default function () {
  const res = http.post(`${BASE_URL}/v1/chat/completions`, body, {
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${API_KEY}` },
  });
  check(res, { 'status 200': (r) => r.status === 200 });
}
