import http from 'k6/http';
import { check, sleep } from 'k6';

// 1. 테스트 전 초기화 로직
export function setup() {
  const productId = 1;
  const adminUrl = `http://localhost:8080/api/v1/admin/products/prepare?productId=1&stockQuantity=100`;

  const res = http.post(adminUrl);

  if (res.status !== 200) {
    console.error('초기 데이터 셋업 실패..');
  } else {
    console.log('초기 데이터 셋업 성공!');
  }
}

export const options = {
  vus: 100,         // 100명의 사용자가
  duration: '30s',  // 30초 동안
};

export default function (data) {
  const productId = 1;
  const userId = __VU * 1000 + __ITER;
  /*const url = `http://localhost:8080/api/v1/orders/${productId}?userId=${userId}&quantity=1`;*/
  const url = `http://localhost:8080/api/v1/orders/lua/${productId}?userId=${userId}&quantity=1`;

  const res = http.post(url, null, { tags: { name: 'OrderAPI' } });

  check(res, {
    'is status 200': (r) => r.status === 200,
  });

  sleep(0.1);
}