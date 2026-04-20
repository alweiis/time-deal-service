package com.example.timedeal.domain.order.consumer;

import com.example.timedeal.domain.order.entity.Order;
import com.example.timedeal.domain.order.event.OrderEvent;
import com.example.timedeal.domain.order.repository.OrderRepository;
import com.example.timedeal.domain.product.service.ProductService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final ProductService productService;
    private final OrderRepository orderRepository;
    private final RedissonClient redissonClient;

    @RetryableTopic(backoff = @Backoff(delay = 2000))
    @Transactional
    @KafkaListener(topics = "order-create-topic", groupId = "time-deal-group")
    public void consumeOrderEvent(
            OrderEvent orderEvent,
            @Header(value = "traceId", required = false) String traceId // 헤더 수신
    ) {
        // 1. 헤더에서 받은 Trace ID를 MDC에 세팅
        String currentTraceId = (traceId != null) ? traceId : "unknown";
        MDC.put("traceId", currentTraceId);

        try {
            log.info("카프카에서 주문 이벤트 수신 완료! Data: {}, TraceID: {}", orderEvent, traceId);

            // [테스트용 강제 예외 발생] productId가 999L이면 실패한다고 가정
            if (orderEvent.productId().equals(999L)) {
                throw new RuntimeException("DB 장애 발생 시뮬레이션!");
            }

            String idempotencyKey = "idempotency:order:" + traceId;
            RBucket<String> bucket = redissonClient.getBucket(idempotencyKey);

            // setIfAbsent: 키가 없으면 "PROCESSED"를 저장하고 true 반환. 이미 있으면 false 반환. (TTL: 1일)
            boolean isNewMessage = bucket.setIfAbsent("PROCESSED", Duration.ofDays(1));

            if (!isNewMessage) {
                // 이미 처리된 TraceID라면 조용히 무시(Skip)하고 메서드 종료
                log.warn("[멱등성 방어] 이미 처리된 주문 이벤트입니다. Skip 처리. TraceID: {}", traceId);
                return;
            }

            processOrder(orderEvent);

            log.info("주문 처리 완료 (상품 ID: {}, 유저 ID: {})", orderEvent.productId(), orderEvent.userId());
        } catch (Exception e) {
            // 인터럽트 상태 복구 (최상위 Exception을 잡을 때의 관례)
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            log.error("주문 처리 중 에러 발생: {}", e.getMessage());
            throw e; // 재시도를 위해 다시 던짐
        } finally {
            MDC.clear();
        }
    }

    private void processOrder(OrderEvent orderEvent) {
        productService.decreaseStock(orderEvent.productId(), orderEvent.quantity());
        Order order = new Order(orderEvent.userId(), orderEvent.productId(), orderEvent.quantity());
        orderRepository.save(order);
    }

    @DltHandler
    public void handleDlt(OrderEvent orderEvent, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.error("=== [DLQ 발생] 모든 재시도 실패, 메시지 격리 완료 ===");
        log.error("Topic: {}, Data: {}", topic, orderEvent);
    }
}
