package com.example.timedeal.domain.product.service;

import com.example.timedeal.domain.order.repository.OrderRepository;
import com.example.timedeal.domain.product.entity.Product;
import com.example.timedeal.domain.product.repository.ProductRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RedissonClient;
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

    @Autowired
    private OptimisticLockFacade optimisticLockFacade;
    private Product savedProduct;
    @Autowired
    private RedissonLockFacade redissonLockFacade;

    @Autowired
    private RedissonClient redissonClient;

    @Autowired
    private OrderRepository orderRepository;

    @BeforeEach
    void setUp() {
        Product product = new Product("아이폰 17", 1500000L, 10L);
        savedProduct = productRepository.saveAndFlush(product);

        RAtomicLong stockCounter = redissonClient.getAtomicLong("product_stock:" + savedProduct.getId());
        stockCounter.set(10L);
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

    @Test
    @DisplayName("비관적 락을 적용하면 100명이 동시에 주문해도 재고가 정확히 0이 된다")
    void decreaseStock_pessimistic_lock() throws InterruptedException {
        // given
        int threadCount = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(32);
        CountDownLatch latch = new CountDownLatch(threadCount);

        long startTime = System.currentTimeMillis();

        // when
        for (int i = 0; i < threadCount; i++) {
            executorService.submit(() -> {
                try {
                    productService.decreaseStockWithPessimisticLock(savedProduct.getId(), 1L);
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        long endTime = System.currentTimeMillis();

        // then
        Product findProduct = productRepository.findById(savedProduct.getId()).orElseThrow();
        System.out.println("===== [비관적 락] 100명 주문 후 남은 실제 재고: " + findProduct.getStockQuantity() + " =====");
        System.out.println("===== [낙관적 락] 실행 시간: " + (endTime - startTime) + " =====");
        assertThat(findProduct.getStockQuantity()).isEqualTo(0L);
    }

    @Test
    @DisplayName("낙관적 락을 적용하면 100명이 동시에 주문해도 재고가 0이 된다")
    void decreaseStock_optimistic_lock() throws InterruptedException {
        int threadCount = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(32);
        CountDownLatch latch = new CountDownLatch(threadCount);

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < threadCount; i++) {
            executorService.submit(() -> {
                try {
                    optimisticLockFacade.decreaseStock(savedProduct.getId(), 1L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        long endTime = System.currentTimeMillis();

        Product findProduct = productRepository.findById(savedProduct.getId()).orElseThrow();
        System.out.println("===== [낙관적 락] 남은 재고: " +  findProduct.getStockQuantity() + " =====");
        System.out.println("===== [낙관적 락] 실행 시간: " + (endTime - startTime) + " =====");
        assertThat(findProduct.getStockQuantity()).isEqualTo(0L);
    }

    @Test
    @DisplayName("Redis 분산 락을 적용하면 100명이 동시에 주문해도 재고가 0이 된다")
    void decreaseStock_redisson_lock() throws InterruptedException {
        // given
        int threadCount = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(32);
        CountDownLatch latch = new CountDownLatch(threadCount);

        long startTime = System.currentTimeMillis();

        // when
        for (int i = 0; i < threadCount; i++) {
            executorService.submit(() -> {
                try {
                    redissonLockFacade.decreaseStock(savedProduct.getId(), 1L);
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        long endTime = System.currentTimeMillis();

        // then
        Product findProduct = productRepository.findById(savedProduct.getId()).orElseThrow();
        System.out.println("====== [Redis 분산 락] 남은 재고: " + findProduct.getStockQuantity() + " ======");
        System.out.println("====== [Redis 분산 락] 실행 시간: " + (endTime - startTime) + "ms ======");

        assertThat(findProduct.getStockQuantity()).isEqualTo(0L);
    }

    @Test
    @DisplayName("최종 테스트: 재고는 0이 되고, 주문 내역은 정확히 10건이 생성되어야 한다")
    void final_time_deal_test() throws InterruptedException {
        //given
        int threadCount = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(32);
        CountDownLatch latch = new CountDownLatch(threadCount);

        //when
        for (int i = 0; i < threadCount; i++) {
            long userId = (long) i;

            executorService.submit(() -> {
                try {
                    // Redis 카운터 -> Kafka 발행 로직이 담긴 파사드 호출
                    redissonLockFacade.decreaseStockAndCreateOrder(savedProduct.getId(), 1L, userId);
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();

        System.out.println("====== 카프카 컨슈머 처리 대기 중 (10초) ======");
        Thread.sleep(10000);

        //then
        Product findProduct = productRepository.findById(savedProduct.getId()).orElseThrow();

        // 검증 1: DB 재고가 정확히 0인지 확인
        System.out.println("===== 최종 DB 남은 재고: " +  findProduct.getStockQuantity() + " =====");
        assertThat(findProduct.getStockQuantity()).isEqualTo(0L);

        // 검증 2: Redis 카운터가 0 이하로 내려갔는지 확인
        RAtomicLong stockCounter = redissonClient.getAtomicLong("product_stock:" + savedProduct.getId());
        System.out.println("===== 최종 Redis 남은 재고: " + stockCounter.get() + " =====");
        assertThat(stockCounter.get()).isEqualTo(0L);

        // 검증 3: 주문 테이블에 데이터가 10개 쌓였는가?
        long orderCount = orderRepository.count();
        System.out.println("====== 생성된 주문 건수: " + orderCount + " ======");
        assertThat(orderCount).isEqualTo(10L);
    }
}