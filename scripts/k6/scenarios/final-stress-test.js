import http from 'k6/http';
import { check, sleep } from 'k6';

// TraceId를 위한 UUID 생성 함수
function uuidv4() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

export const options = {
  stages: [
    { duration: '5s', target: 20 },   // 5초 동안 20명으로 증가
    { duration: '20s', target: 100 }, // 20초 동안 100명 유지
    { duration: '5s', target: 0 },    // 5초 동안 0명으로 감소
  ],
};

export default function () {
  const productId = 1;
  const url = `http://localhost:8080/api/v1/orders/lua/${productId}`;
  const payload = JSON.stringify({
    quantity: 1,
    userId: Math.floor(Math.random() * 1000) // 0~999 사이의 랜덤 유저 ID
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'traceId': uuidv4(), // 매 요청마다 새로운 멱등성 키 발급
    },
  };

  const res = http.post(url, payload, params);

  if (res.status !== 200) {
    console.error(`[요청 실패] Status: ${res.status}, 응답: ${res.body}`);
  }

  check(res, {
    'is status 200': (r) => r.status === 200,
  });

  sleep(0.1);
}