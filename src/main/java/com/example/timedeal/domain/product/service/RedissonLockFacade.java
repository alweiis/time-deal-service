package com.example.timedeal.domain.product.service;

import com.example.timedeal.domain.order.event.OrderEvent;
import com.example.timedeal.domain.order.producer.OrderEventProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class RedissonLockFacade {

    private final RedissonClient redissonClient;
    private final OrderEventProducer orderEventProducer;

    public void decreaseStock(Long productId, Long quantity, Long userId) {
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
            OrderEvent event = new OrderEvent(userId, productId, quantity);
            orderEventProducer.sendOrderEvent(event);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 4. 로직이 다 끝났거나 예외가 발생해도 반드시 락을 해제
            if (lock.isLocked() && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

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
}
