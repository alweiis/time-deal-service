package com.example.timedeal.domain.order.event;

public record OrderEvent(
        Long userId,
        Long productId,
        Long quantity
) {
}
