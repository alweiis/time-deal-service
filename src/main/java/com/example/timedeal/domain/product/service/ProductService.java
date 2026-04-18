package com.example.timedeal.domain.product.service;

import com.example.timedeal.common.exception.BusinessException;
import com.example.timedeal.common.exception.ErrorCode;
import com.example.timedeal.domain.product.entity.Product;
import com.example.timedeal.domain.product.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProductService {

    private final ProductRepository productRepository;
    private final RedissonClient redissonClient;

    @Transactional
    public void decreaseStock(Long id, Long quantity) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new BusinessException(ErrorCode.PRODUCT_NOT_FOUND));
        product.decreaseStock(quantity);
    }

    @Transactional
    public void decreaseStockWithPessimisticLock(Long id, Long quantity) {
        Product product = productRepository.findByIdWithPessimisticLock(id)
                .orElseThrow(() -> new BusinessException(ErrorCode.PRODUCT_NOT_FOUND));

        product.decreaseStock(quantity);

        productRepository.saveAndFlush(product);
    }

    @Transactional
    public void decreaseStockWithOptimisticLock(Long id, Long quantity) {
        Product product = productRepository.findByIdWithOptimisticLock(id)
                .orElseThrow(() -> new BusinessException(ErrorCode.PRODUCT_NOT_FOUND));
        product.decreaseStock(quantity);
        productRepository.saveAndFlush(product);
    }

    @Transactional
    public void prepareProductForTest(Long productId, String name, Long price, Long stockQuantity) {
        // 1. DB에 상품이 있는지 확인하고, 없으면 생성
        Product product = productRepository.findById(productId)
                .orElseGet(() -> {
                    // 존재하지 않으면 새로 생성
                    return productRepository.save(new Product(name, price, stockQuantity));
                });

        // 2. Redis 카운터 초기화 (DB 재고와 완전히 일치시킴)
        RAtomicLong stockCounter = redissonClient.getAtomicLong("product_stock:" + product.getId());
        stockCounter.set(product.getStockQuantity());

        log.info("테스트 준비 완료 - 상품 ID: {}, 이름: {}, DB/Redis 재고: {}",
                product.getId(), product.getName(), product.getStockQuantity());
    }
}
