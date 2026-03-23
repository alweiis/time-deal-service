package com.example.timedeal.domain.product.service;

import com.example.timedeal.domain.product.entity.Product;
import com.example.timedeal.domain.product.repository.ProductRepository;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class ProductServiceTest {

    @Autowired
    private ProductService productService;

    @Autowired
    private ProductRepository productRepository;
    private Product savedProduct;

    @BeforeEach
    void setUp() {
        Product product = new Product("아이폰 17", 1500000L, 100L);
        savedProduct = productRepository.saveAndFlush(product);
    }

    @AfterEach
    void tearDown() {
        productRepository.deleteAll();
    }

    @Test
    @DisplayName("동시에 100명이 1개씩 주문하면 재고가 0이 되어야 한다(하지만 실패)")
    void decreaseStock_concurrency_issue() throws InterruptedException {
        // given
        int threadCount = 100;

        // 멀티 스레드 환경을 만들기 위한 스래드 생성
        ExecutorService executorService = Executors.newFixedThreadPool(32);

        // 100개의 요청이 모두 끝날 때까지 메인 스레드가 기다려주기 위한 장치
        CountDownLatch latch = new CountDownLatch(threadCount);

        // when: 100번의 요청을 동시에 요청한다
        for (int i = 0; i < threadCount; i++) {
            executorService.submit(() -> {
                try {
                    productService.decreaseStock(savedProduct.getId(), 1L);
                } finally {
                    latch.countDown();
                }
            });
        }
        // 카운트가 0이 될 때까지 대기
        latch.await();

        // then
        Product findProduct = productRepository.findById(savedProduct.getId()).orElseThrow();

        // 콘솔에 실제 남은 재고 출력
        System.out.println("===== 100명 주문 후 남은 실제 재고: " + findProduct.getStockQuantity() + " =====");

        // 정상이라면 100개 중 1씩 100번을 뺐으니 0이 되어야 한다
        assertThat(findProduct.getStockQuantity()).isEqualTo(0L);
    }
}