package com.example.timedeal.domain.order.consumer;

import com.example.timedeal.domain.order.entity.Order;
import com.example.timedeal.domain.order.event.OrderEvent;
import com.example.timedeal.domain.order.repository.OrderRepository;
import com.example.timedeal.domain.product.service.ProductService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final ProductService productService;
    private final OrderRepository orderRepository;

    @Transactional
    @KafkaListener(topics = "order-create-topic", groupId = "time-deal-group")
    public void consumeOrderEvent(OrderEvent orderEvent) {
        log.info("카프카에서 주문 이벤트 수신 완료! Data: {}", orderEvent);

        try {
            // 1. 실재 DB의 재고 차감
            productService.decreaseStock(orderEvent.productId(),  orderEvent.quantity());

            // 2. 주문 내역 생성 및 저장
            Order order = new Order(orderEvent.userId(), orderEvent.productId(), orderEvent.quantity());
            orderRepository.save(order);

            log.info("주문 처리 완료 (상품 ID: {}, 유저 ID: {})", orderEvent.productId(), orderEvent.quantity());
        } catch (Exception e) {
            log.error("주문 처리 중 에러 발생: {}", e.getMessage());
            throw e;
        }
    }
}
