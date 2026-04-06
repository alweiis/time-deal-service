package com.example.timedeal.domain.product.service;

import com.example.timedeal.domain.product.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class OptimisticLockFacade {

    private final ProductRepository productRepository;
    private final ProductService productService;

    // 낙관적 락 충돌 시 재시도(retry)를 수행하는 파사드 로직
    public void decreaseStock(Long id, Long quantity) throws InterruptedException {
        while (true) {
            try {
                productService.decreaseStockWithOptimisticLock(id, quantity);
                break;
            } catch (OptimisticLockingFailureException e) {
                Thread.sleep(50);
            }
        }
    }
}
