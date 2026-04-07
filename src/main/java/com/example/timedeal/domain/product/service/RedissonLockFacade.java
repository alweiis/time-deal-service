package com.example.timedeal.domain.product.service;

import com.example.timedeal.domain.order.event.OrderEvent;
import com.example.timedeal.domain.order.producer.OrderEventProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
}
