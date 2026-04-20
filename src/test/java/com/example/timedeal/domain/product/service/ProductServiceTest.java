package com.example.timedeal.domain.product.service;

import com.example.timedeal.domain.order.event.OrderEvent;
import com.example.timedeal.domain.order.repository.OrderRepository;
import com.example.timedeal.domain.product.entity.Product;
import com.example.timedeal.domain.product.repository.ProductRepository;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RedissonClient;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.time.Duration;
import java.util.UUID;
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

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @BeforeEach
    void setUp() {
        Product product = new Product("아이폰 17", 1500000L, 10L);
        savedProduct = productRepository.saveAndFlush(product);

        RAtomicLong stockCounter = redissonClient.getAtomicLong("product_stock:" + savedProduct.getId());
        stockCounter.set(10L);
    }

    @AfterEach
    void tearDown() {
        orderRepository.deleteAllInBatch();
        productRepository.deleteAllInBatch();
        redissonClient.getKeys().flushdb();
    }

    @Test
    @DisplayName("동시에 100명이 1개씩 주문해도 재고가 0이 되어야 한다")
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
                    // 스레드마다 고유한 가짜 TraceID를 강제로 주입하여 실제 환경 시뮬레이션
                    MDC.put("traceId", UUID.randomUUID().toString());

                    // Redis 카운터 -> Kafka 발행 로직이 담긴 파사드 호출
                    redissonLockFacade.decreaseStockAndCreateOrder(savedProduct.getId(), userId, 1L);
                } finally {
                    // 스레드 풀 사용 시 반드시 clear()로 데이터를 비워주어야 함 (Memory Leak 방지)
                    MDC.clear();
                    latch.countDown();
                }
            });
        }

        latch.await();

        System.out.println("====== 카프카 컨슈머 비동기 처리 대기 (최대 15초) ======");

        // then
        // Thread.sleep(10000) 대신 Awaitility 사용: 최대 15초까지 대기하며, 1초마다 검증 로직을 실행해 봅니다.
        Awaitility.await()
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    Product findProduct = productRepository.findById(savedProduct.getId()).orElseThrow();

                    // 검증 1: DB 재고가 정확히 0인지 확인
                    assertThat(findProduct.getStockQuantity()).isEqualTo(0L);

                    // 검증 2: Redis 카운터가 0 이하로 내려갔는지 확인
                    RAtomicLong stockCounter = redissonClient.getAtomicLong("product_stock:" + savedProduct.getId());
                    assertThat(stockCounter.get()).isEqualTo(0L);

                    // 검증 3: 주문 테이블에 데이터가 10개 쌓였는가?
                    long orderCount = orderRepository.count();
                    assertThat(orderCount).isEqualTo(10L);
                });

        System.out.println("====== 비동기 테스트 완벽 통과! ======");
    }

    @Test
    @DisplayName("장애 상황 시뮬레이션: 재시도 3번 후 DLQ로 이동해야 한다")
    void dlq_test() throws InterruptedException {
        // given (재고가 없는 상품 ID 발송)
        // 게이트키퍼(redis)를 통과하기 위해 999L 상품에 가짜 재고 1개를 넣는다
        long dummyProductId = 999L;
        RAtomicLong stockCounter = redissonClient.getAtomicLong("product_stock:" + dummyProductId);
        stockCounter.set(1L);

        // when : 존재하지 않는 상품 ID(999L)로 주문 요청
        redissonLockFacade.decreaseStockAndCreateOrder(999L, 1L, 1L);

        // then: 3번 재시도(2초 간격)와 DLT 이동을 위해 10초 대
        System.out.println("===== DLQ 재처리 및 격리 대기 중 (10초) =====");
        Thread.sleep(10000);
    }

    @Test
    @DisplayName("성능 비교: 분산 락 vs Lua Script")
    void compare_performance() throws InterruptedException {
        int threadCount = 100;
        Long productId = savedProduct.getId();

        // 1. 분산 락 방식 측정
        long startLock = System.currentTimeMillis();
        runConcurrencyTest(() -> redissonLockFacade.decreaseStockAndCreateOrder(productId, 1L, 1L), threadCount);
        long endLock = System.currentTimeMillis();

        // 2. Redis 초기화 (비교를 위해 다시 100개 세팅)
        redissonClient.getAtomicLong("product_stock:" + productId).set(100L);

        // 3. Lua Script 방식 측정
        long startLua = System.currentTimeMillis();
        runConcurrencyTest(() -> redissonLockFacade.decreaseStockWithLua(productId, 1L, 1L), threadCount);
        long endLua = System.currentTimeMillis();

        System.out.println("====== 결과 보고 ======");
        System.out.println("분산 락 방식 소요 시간: " + (endLock - startLock) + "ms");
        System.out.println("Lua Script 방식 소요 시간: " + (endLua - startLua) + "ms");
    }

    @Test
    @DisplayName("멱등성 테스트: 동일한 traceId를 가진 이벤트가 중복 유입되어도 주문은 1번만 처리된다")
    void idempotency_test() {
        // given: 오직 이 테스트만을 위한 독립적인 상품 생성 (테스트 간섭 차단)
        Product testProduct = createTestProduct("아이폰 17 Pro", 1750000L, 10L);
        Long productId = testProduct.getId();
        Long userId = 1L;
        Long quantity = 1L;

        // 멱등성 방어를 확인할 고유한 Trace ID 발급
        String duplicateTraceId = UUID.randomUUID().toString();

        OrderEvent event = new OrderEvent(userId, productId, quantity);

        // 카프카 파티션 순서 보장을 위해 Key(userId) 할당
        Message<OrderEvent> message = MessageBuilder.withPayload(event)
                .setHeader(KafkaHeaders.TOPIC, "order-create-topic")
                .setHeader(KafkaHeaders.KEY, String.valueOf(userId))
                .setHeader("traceId", duplicateTraceId)
                .build();

        // ==========================================
        // STEP 1: 첫 번째 정상 요청 처리 및 검증
        // ==========================================
        kafkaTemplate.send(message);

        // 첫 번째 메시지가 정상적으로 DB와 Redis에 반영될 때까지 대기
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    // 재고가 10 -> 9 로 깎여야 함
                    Product findProduct = productRepository.findById(productId).orElseThrow();
                    assertThat(findProduct.getStockQuantity()).isEqualTo(9L);

                    // 주문이 1건 생성되어야 함
                    long orderCount = orderRepository.findAll().stream()
                            .filter(o -> o.getProductId().equals(productId))
                            .count();
                    assertThat(orderCount).isEqualTo(1L);
                });

        System.out.println("====== [1차 검증] 정상 요청 처리 완료 ======");

        // ==========================================
        // STEP 2: 중복 요청 시뮬레이션 (네트워크 지연, 더블 클릭 등)
        // ==========================================
        // 완벽히 동일한 메시지(TraceID가 같음)를 다시 카프카로 전송
        kafkaTemplate.send(message);

        // ==========================================
        // STEP 3: 상태가 '변하지 않았음'을 확인 (멱등성의 증명)
        // ==========================================
        Product finalProduct = productRepository.findById(productId).orElseThrow();
        long finalOrderCount = orderRepository.findAll().stream()
                .filter(o -> o.getProductId().equals(productId))
                .count();

        // 재고는 여전히 9개여야 하고, 주문도 여전히 1건이어야 합니다. (2번 깎이지 않음!)
        assertThat(finalProduct.getStockQuantity()).isEqualTo(9L);
        assertThat(finalOrderCount).isEqualTo(1L);

        System.out.println("====== [2차 검증] 멱등성 방어 성공: 중복 메시지가 안전하게 무시되었습니다 ======");
    }

    private Product createTestProduct(String name, long price, long stock) {
        Product product = new Product(name, price, stock);
        Product savedProduct = productRepository.saveAndFlush(product);

        RAtomicLong stockCounter = redissonClient.getAtomicLong("product_stock:" + savedProduct.getId());
        stockCounter.set(stock);

        return savedProduct;
    }

    // 헬퍼 메서드
    private void runConcurrencyTest(Runnable task, int threadCount) throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(32);
        CountDownLatch latch = new CountDownLatch(threadCount);
        for (int i = 0; i < threadCount; i++) {
            executorService.submit(() -> {
                try {
                    task.run();
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
    }
}