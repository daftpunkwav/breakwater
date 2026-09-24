// quota-race: concurrent drains of one tenant's balance; afterwards the
// admin balance query must reconcile exactly against the summed usage
// (invariants I3/I9, the load-level complement of the -race unit test).
//
//   k6 run -e BASE_URL=... -e API_KEY=... -e ADMIN_TOKEN=... quota-race.js
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1:8080';
const API_KEY = __ENV.API_KEY || 'bw-local-t2';
const ADMIN_TOKEN = __ENV.ADMIN_TOKEN || '';

export const options = {
  scenarios: {
    drain: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 200),
      timeUnit: '1s',
      duration: __ENV.DURATION || '30s',
      preAllocatedVUs: 100,
      maxVUs: 500,
    },
  },
};

const body = JSON.stringify({
  model: 'mock-gpt',
  temperature: 0.7, // bypass cache: every request must settle against usage
  max_tokens: 32,
  messages: [{ role: 'user', content: 'drain the balance' }],
});

export default function () {
  const res = http.post(`${BASE_URL}/v1/chat/completions`, body, {
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${API_KEY}` },
  });
  check(res, { 'served or honestly denied': (r) => r.status === 200 || r.status === 402 });
}

export function teardown() {
  const headers = ADMIN_TOKEN ? { Authorization: `Bearer ${ADMIN_TOKEN}` } : {};
  const res = http.get(`${BASE_URL}/admin/tenants/${'local-2'}/quota`, { headers });
  console.log(`final balance: ${res.body}`);
  // Reconciliation: initial balance - final balance must equal the sum
  // of every settled usage (read access log or mockllm counters).
}
