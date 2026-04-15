package com.example.timedeal.domain.product.service;

import com.example.timedeal.domain.order.event.OrderEvent;
import com.example.timedeal.domain.order.producer.OrderEventProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RLock;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class RedissonLockFacade {

    private final RedissonClient redissonClient;
    private final ProductService productService;
    private final OrderEventProducer orderEventProducer;

    // Redis 분산 락 획득 후 DB에 직접 동기(Sync) 업데이트
    public void decreaseStock(Long productId, Long quantity) {
        // 1. Redis에서 락 객체 가져오기
        RLock lock = redissonClient.getLock("product_lock:" + productId);

        try {
            // 2. 락 획득 시도 (최대 10초 대기, 락 획득 후 1초 지나면 자동 해제)
            boolean available = lock.tryLock(10, 1, TimeUnit.SECONDS);

            if (!available) {
                log.info("lock 획득 실패 (상품 ID: {})", productId);
                return;
            }

            // 3. 락 획득에 성공했다면, 실제 트랜잭션이 걸린 비즈니스 로직 호출
            productService.decreaseStock(productId, quantity);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 4. 로직이 다 끝났거나 예외가 발생해도 반드시 락을 해제
            if (lock.isLocked() && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    // Redis 카운터(게이트키퍼) 확인 후 Kafka 비동기(Async) 이벤트 발행
    public void decreaseStockAndCreateOrder(Long productId, Long quantity, Long userId) {
        RLock lock = redissonClient.getLock("product_lock:" + productId);

        try {
            // 1. 락 획득 (여러 서버에서 동시에 Redis 재고를 깎는 것을 방지)
            boolean available = lock.tryLock(10, 1, TimeUnit.SECONDS);

            if (!available) {
                return;
            }

            // 2. Redis에 저장된 재고 수량을 가져옴 (미리 저장되어 있다고 가정)
            RAtomicLong stockCounter = redissonClient.getAtomicLong("product_stock:" + productId);

            // 3. Redis에서 먼저 재고를 깎음 (Atomic 연산)
            long remainStock = stockCounter.decrementAndGet();

            if (remainStock < 0) {
                // 재고가 없으면 깎았던 숫자를 다시 복구시키고 종료 (Rollback)
                stockCounter.incrementAndGet();
                log.info("품절되었습니다. (상품 ID: {})", productId);
                return;
            }

            // 4. 재고가 확인된 경우에만 Kafka로 주문 이벤트 발행
            OrderEvent event = new OrderEvent(userId, productId, quantity);
            orderEventProducer.sendOrderEvent(event);
            log.info("주문 이벤트 발행 성공 (남은 Redis 재고: {})", remainStock);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            if (lock.isLocked() && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    public void decreaseStockWithLua(Long productId, Long quantity, Long userId) {
        String stockKey = "product_stock:" + productId;

        // 1. Lua Script 정의
        String script =
                "local stock = redis.call('get', KEYS[1]) " +
                        "if not stock then return -1 end " +
                        "if tonumber(stock) >= tonumber(ARGV[1]) then " +
                        "   return redis.call('decrby', KEYS[1], ARGV[1]) " +
                        "else " +
                        "   return -2 " +
                        "end";

        // 2. 스크립트 실행
        // 핵심 포인트: StringCodec.INSTANCE를 주입하고, quantity도 String으로 변환해서 전달
        Object evalResult = redissonClient.getScript(StringCodec.INSTANCE)
                .eval(RScript.Mode.READ_WRITE,
                        script,
                        RScript.ReturnType.VALUE, // INTEGER 대신 VALUE 사용
                        java.util.Collections.singletonList(stockKey),
                        quantity.toString()); // Long을 순수 String으로 전달

        // 3. 반환값 처리 (Object를 Long으로 안전하게 캐스팅)
        Long result = Long.valueOf(evalResult.toString());

        if (result >= 0) {
            OrderEvent event = new OrderEvent(userId, productId, quantity);
            orderEventProducer.sendOrderEvent(event);
            log.info("[Lua] 주문 이벤트 발행 성공 (남은 재고: {})", result);
        } else if (result == -2) {
            log.info("[Lua] 품절되었습니다. 상품 ID: {})", productId);
        } else {
            log.error("[Lua] Redis에 재고 데이터가 존재하지 않습니다. (ID: {})", productId);
        }
    }
}
