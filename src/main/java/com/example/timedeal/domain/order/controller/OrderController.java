package com.example.timedeal.domain.order.controller;

import com.example.timedeal.common.response.ApiResponse;
import com.example.timedeal.domain.order.dto.OrderCreateRequest;
import com.example.timedeal.domain.product.service.RedissonLockFacade;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
public class OrderController {

    private final RedissonLockFacade redissonLockFacade;

    @PostMapping("/{productId}")
    public ApiResponse<String> createOrder(
            @PathVariable Long productId,
            @Valid @RequestBody OrderCreateRequest request
            ) {
        redissonLockFacade.decreaseStockAndCreateOrder(productId, request.getUserId(), request.getQuantity());
        return ApiResponse.success("주문 요청이 성공적으로 접수되었습니다. (분산 락)");
    }

    /**
     * Lua Script 기반 타임딜 주문 API (성능 최적화 버전)
     */
    @PostMapping("/lua/{productId}")
    public ApiResponse<String> createOrderWithLua(
            @PathVariable Long productId,
            @Valid @RequestBody OrderCreateRequest request
    ) {
        redissonLockFacade.decreaseStockWithLua(productId, request.getUserId(), request.getQuantity());
        return ApiResponse.success("주문 요청이 성공적으로 접수되었습니다. (Lua Script)");
    }
}
