// cache-hit: identical deterministic requests; measures hit rate and
// the hit-vs-fetch latency gap (invariant I2 economics).
//
//   k6 run -e BASE_URL=... -e API_KEY=... cache-hit.js
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1:8080';
const API_KEY = __ENV.API_KEY || 'bw-local-t1';

export const options = {
  scenarios: {
    hot: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 300),
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: 100,
      maxVUs: 500,
    },
  },
};

// One identical deterministic body for every VU: after the single cold
// fetch, every response must come from the cache.
const body = JSON.stringify({
  model: 'mock-gpt',
  temperature: 0,
  messages: [{ role: 'user', content: 'deterministic prompt' }],
});

export default function () {
  const res = http.post(`${BASE_URL}/v1/chat/completions`, body, {
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${API_KEY}` },
  });
  check(res, { 'status 200': (r) => r.status === 200 });
}
// Hit rate and fetch count are read from the gateway afterwards:
//   curl -s $BASE_URL/metrics | grep -E 'cache_(hit|miss|upstream_fetch)'
// I2 assertion: upstream_fetch_total == 1 regardless of request count.
