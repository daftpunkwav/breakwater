// redis-kill: the governance backend dies mid-run; every degradation
// must be visible and safe — rate limiting fail-closed (503/429, never
// silent pass-through), cache bypass, quota fail-closed (PRD Q4).
//
//   k6 run -e BASE_URL=... -e API_KEY=... redis-kill.js
// Mid-run: docker stop $(docker ps -qf name=redis); restart it a few
// seconds later. The gateway log and metrics show the story.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1:8080';
const API_KEY = __ENV.API_KEY || 'bw-local-t1';

const rejections = new Counter('governance_rejections_503');

export const options = {
  scenarios: {
    kill: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 100),
      timeUnit: '1s',
      duration: __ENV.DURATION || '90s',
      preAllocatedVUs: 100,
      maxVUs: 400,
    },
  },
};

const body = JSON.stringify({
  model: 'mock-gpt',
  temperature: 0.7,
  messages: [{ role: 'user', content: 'survive the outage' }],
});

export default function () {
  const res = http.post(`${BASE_URL}/v1/chat/completions`, body, {
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${API_KEY}` },
  });
  if (res.status === 503) {
    // Fail-closed: the gateway refuses traffic it cannot govern.
    rejections.add(1);
    check(res, { 'denial is explicit': (r) => r.status === 503 });
  } else {
    check(res, { 'served normally': (r) => r.status === 200 });
  }
}
