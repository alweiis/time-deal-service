package com.example.timedeal.domain.order.producer;

import com.example.timedeal.domain.order.event.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventProducer {

    private final KafkaTemplate<String , Object> kafkaTemplate;

    public void sendOrderEvent(OrderEvent orderEvent){
        String topic = "order-create-topic";

        kafkaTemplate.send(topic, orderEvent);
        log.info("카프카로 주문 이벤트 전송 완료: Topic: {}, Data: {}", topic, orderEvent);
    }
}
