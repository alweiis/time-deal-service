package com.example.timedeal.domain.product.service;

import com.example.timedeal.domain.product.entity.Product;
import com.example.timedeal.domain.product.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class ProductService {

    private final ProductRepository productRepository;

    @Transactional
    public void decreaseStock(Long id, Long quantity) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("상품이 존재하지 않습니다."));
        product.decreaseStock(quantity);
    }

    @Transactional
    public void decreaseStockWithPessimisticLock(Long id, Long quantity) {
        Product product = productRepository.findByIdWithPessimisticLock(id)
                .orElseThrow(() -> new IllegalArgumentException("상품이 존재하지 않습니다."));

        product.decreaseStock(quantity);

        productRepository.saveAndFlush(product);
    }

    @Transactional
    public void decreaseStockWithOptimisticLock(Long id, Long quantity) {
        Product product = productRepository.findByIdWithOptimisticLock(id)
                .orElseThrow(() -> new IllegalArgumentException("상품이 존재하지 않습니다."));
        product.decreaseStock(quantity);
        productRepository.saveAndFlush(product);
    }
}
