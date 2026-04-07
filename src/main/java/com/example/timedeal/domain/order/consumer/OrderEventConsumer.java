package com.example.timedeal.domain.order.consumer;

import com.example.timedeal.domain.order.event.OrderEvent;
import com.example.timedeal.domain.product.service.ProductService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final ProductService productService;

    @KafkaListener(topics = "order-create-topic", groupId = "time-deal-group")
    public void consumeOrderEvent(OrderEvent orderEvent) {
        log.info("카프카에서 주문 이벤트 수신 완료! Data: {}", orderEvent);

        try {
            productService.decreaseStock(orderEvent.productId(),  orderEvent.quantity());
            log.info("DB 재고 차감 완료 (상품 ID: {})", orderEvent.productId());
        } catch (Exception e) {
            log.error("재고 차감 중 에러 발생: {}", e.getMessage());
        }
    }
}
