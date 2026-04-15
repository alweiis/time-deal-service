package com.example.timedeal.domain.order.producer;

import com.example.timedeal.domain.order.event.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;


@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventProducer {

    private final KafkaTemplate<String , Object> kafkaTemplate;

    public void sendOrderEvent(OrderEvent orderEvent){
        String traceId = MDC.get("traceId");
        String topic = "order-create-topic";

        // ProducerRecord를 생성하여 헤더에 전달
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, orderEvent);
        record.headers().add("traceId", traceId != null ? traceId.getBytes() : "system".getBytes());

        kafkaTemplate.send(record);
        log.info("카프카로 주문 이벤트 전송 완료: Topic: {}, Data: {}", topic, orderEvent);
    }
}
