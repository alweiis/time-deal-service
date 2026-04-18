package com.example.timedeal.domain.order.consumer;

import com.example.timedeal.domain.order.entity.Order;
import com.example.timedeal.domain.order.event.OrderEvent;
import com.example.timedeal.domain.order.repository.OrderRepository;
import com.example.timedeal.domain.product.service.ProductService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final ProductService productService;
    private final OrderRepository orderRepository;

    @RetryableTopic(backoff = @Backoff(delay = 2000))
    @Transactional
    @KafkaListener(topics = "order-create-topic", groupId = "time-deal-group")
    public void consumeOrderEvent(
            OrderEvent orderEvent,
            @Header(value = "traceId", required = false) byte[] traceIdBytes // 헤더 수신
    ) {
        // 1. 헤더에서 받은 Trace ID를 MDC에 세팅
        String traceId = traceIdBytes != null ? new String(traceIdBytes) : "unknown";
        MDC.put("traceId", traceId);

        log.info("카프카에서 주문 이벤트 수신 완료! Data: {}", orderEvent);

        // [테스트용 강제 예외 발생] productId가 999L이면 실패한다고 가정
        if (orderEvent.productId().equals(999L)) {
            throw new RuntimeException("DB 장애 발생 시뮬레이션!");
        }

        try {
            log.info("[Consumer] 주문 처리 시작. TraceID: {}", traceId);
            // 1. 실재 DB의 재고 차감
            productService.decreaseStock(orderEvent.productId(),  orderEvent.quantity());

            // 2. 주문 내역 생성 및 저장
            Order order = new Order(orderEvent.userId(), orderEvent.productId(), orderEvent.quantity());
            orderRepository.save(order);

            log.info("주문 처리 완료 (상품 ID: {}, 유저 ID: {})", orderEvent.productId(), orderEvent.userId());
        } catch (Exception e) {
            log.error("주문 처리 중 에러 발생: {}", e.getMessage());
            throw e;
        } finally {
            MDC.clear();
        }
    }

    @DltHandler
    public void handleDlt(OrderEvent orderEvent, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.error("=== [DLQ 발생] 모든 재시도 실패, 메시지 격리 완료 ===");
        log.error("Topic: {}, Data: {}", topic, orderEvent);
    }
}
