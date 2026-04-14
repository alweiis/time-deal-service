package com.example.timedeal.domain.order.controller;

import com.example.timedeal.domain.product.service.RedissonLockFacade;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
public class OrderController {

    private final RedissonLockFacade redissonLockFacade;

    @PostMapping("/{productId}")
    public String createOrder(
            @PathVariable Long productId,
            @RequestParam Long userId,
            @RequestParam Long quantity
    ) {
        redissonLockFacade.decreaseStockAndCreateOrder(productId, userId, quantity);
        return "주문 요청 접수";
    }
}
