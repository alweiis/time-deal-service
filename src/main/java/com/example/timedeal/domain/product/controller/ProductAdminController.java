package com.example.timedeal.domain.product.controller;

import com.example.timedeal.domain.product.service.ProductService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/admin/products")
@RequiredArgsConstructor
public class ProductAdminController {

    private final ProductService productService;

    @PostMapping("/prepare")
    public String prepareProduct(
            @RequestParam(defaultValue = "1") Long productId,
            @RequestParam(defaultValue = "테스트 상품") String name,
            @RequestParam(defaultValue = "10000") Long price,
            @RequestParam(defaultValue = "100") Long stockQuantity
    ) {
        productService.prepareProductForTest(productId, name, price, stockQuantity);
        return "테스트 데이터 준비 완료 (DB & Redis)";
    }
}
