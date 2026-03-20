package com.loopers.application.payment;

import com.loopers.domain.brand.BrandService;
import com.loopers.domain.order.OrderModel;
import com.loopers.domain.order.OrderService;
import com.loopers.domain.order.OrderStatus;
import com.loopers.domain.payment.PaymentRepository;
import com.loopers.domain.payment.PaymentStatus;
import com.loopers.domain.product.ProductModel;
import com.loopers.domain.product.ProductService;
import com.loopers.domain.product.ProductValidationRequest;
import com.loopers.domain.product.Quantity;
import com.loopers.infrastructure.payment.PgPaymentStatusResponse;
import com.loopers.infrastructure.payment.PgSimulatorClient;
import com.loopers.support.error.CoreException;
import com.loopers.support.error.ErrorType;
import com.loopers.testcontainers.MySqlTestContainersConfig;
import com.loopers.utils.DatabaseCleanUp;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 역할: 콜백 미수신 등으로 남은 PENDING을 {@link PaymentFacade#recoverPendingFromPgSimulator}로 PG 조회 API에 맞춰 정리한다.
 * - SUCCESS → 주문 PAID·결제 SUCCESS (콜백 경로와 동일한 완료 흐름).
 * - FAILED → 결제 FAILED, 주문 ORDERED 유지.
 * - PG 응답 null → 미접수 근사로 결제 TIMEOUT.
 * - PG 예외 → 호출은 삼키고 PENDING 유지(재시도 여지).
 */
@SpringBootTest
@Import(MySqlTestContainersConfig.class)
class PaymentRecoverPollIntegrationTest {

    private static final long USER_ID = 1L;
    private static final String CB = "http://localhost:8080/api/v1/payments/callback";

    @Autowired
    private PaymentFacade paymentFacade;
    @Autowired
    private PaymentPersistenceService persistenceService;
    @Autowired
    private OrderService orderService;
    @Autowired
    private BrandService brandService;
    @Autowired
    private ProductService productService;
    @Autowired
    private PaymentRepository paymentRepository;
    @Autowired
    private DatabaseCleanUp databaseCleanUp;
    @Autowired
    private CircuitBreakerRegistry circuitBreakerRegistry;

    @MockBean
    private PgSimulatorClient pgSimulatorClient;

    @AfterEach
    void tearDown() {
        circuitBreakerRegistry.circuitBreaker("pgCircuit").transitionToClosedState();
        databaseCleanUp.truncateAllTables();
    }

    private OrderModel createOrderedOrder() {
        Long brandId = brandService.registerBrand("recover-brand").getId();
        ProductModel product = productService.registerProduct(brandId, "recover-p", new BigDecimal("9000"), 12);
        return orderService.create(USER_ID, List.of(
                new ProductValidationRequest(product.getId(), Quantity.of(1), null)));
    }

    /** 폴링으로 PG가 이미 성공 처리한 건을 내부 상태에 반영하는 해피 패스. */
    @Test
    @DisplayName("PG 조회가 SUCCESS면 completePayment·결제 SUCCESS로 반영된다.")
    void recoverOrPoll_whenPgReturnsSuccess_shouldReflectCompletePayment() {
        // given
        OrderModel order = createOrderedOrder();
        persistenceService.savePendingAndGetRequestParam(USER_ID, order.getId(), "SAMSUNG", "1", CB);
        long amountWon = order.getFinalAmount().setScale(0, java.math.RoundingMode.HALF_UP).longValue();
        when(pgSimulatorClient.getPaymentsByOrderId(order.getId()))
                .thenReturn(new PgPaymentStatusResponse("pg-poll-1", order.getId(), true, "SUCCESS", amountWon, null));

        // when
        paymentFacade.recoverPendingFromPgSimulator(order.getId());

        // then
        OrderModel after = orderService.findById(USER_ID, order.getId()).orElseThrow();
        assertThat(after.getStatus()).isEqualTo(OrderStatus.PAID);
        assertThat(paymentRepository.findTopByOrderIdOrderByCreatedAtDesc(order.getId()).orElseThrow().getStatus())
                .isEqualTo(PaymentStatus.SUCCESS);
    }

    /** PG가 명시적 실패를 반환하면 결제만 FAILED로 닫고 주문은 주문 확정 상태 유지. */
    @Test
    @DisplayName("PG 조회가 실패(false)면 결제 FAILED·주문 ORDERED다.")
    void recoverOrPoll_whenPgReturnsFailure_shouldMarkFailed() {
        // given
        OrderModel order = createOrderedOrder();
        persistenceService.savePendingAndGetRequestParam(USER_ID, order.getId(), "SAMSUNG", "1", CB);
        when(pgSimulatorClient.getPaymentsByOrderId(order.getId()))
                .thenReturn(new PgPaymentStatusResponse("pg-poll-2", order.getId(), false, "FAILED", null, "LIMIT"));

        // when
        paymentFacade.recoverPendingFromPgSimulator(order.getId());

        // then
        OrderModel after = orderService.findById(USER_ID, order.getId()).orElseThrow();
        assertThat(after.getStatus()).isEqualTo(OrderStatus.ORDERED);
        assertThat(paymentRepository.findTopByOrderIdOrderByCreatedAtDesc(order.getId()).orElseThrow().getStatus())
                .isEqualTo(PaymentStatus.FAILED);
    }

    /** 복구 API가 불필요한 PG 트래픽을 내지 않도록 가드. */
    @Test
    @DisplayName("PENDING이 없으면 PG 조회를 호출하지 않는다.")
    void recoverOrPoll_whenNoPending_shouldNotCallPg() {
        // given
        OrderModel order = createOrderedOrder();

        // when
        paymentFacade.recoverPendingFromPgSimulator(order.getId());

        // then
        verify(pgSimulatorClient, never()).getPaymentsByOrderId(anyLong());
    }

    /** orderId로 PG에 기록이 없음(null) → 요청 미도달/타임아웃 등으로 PENDING을 TIMEOUT 처리. */
    @Test
    @DisplayName("PG 조회 응답이 null이면 PENDING을 TIMEOUT으로 바꾼다 (06 §11.4 미접수 근사).")
    void recoverOrPoll_whenPgReturnsNull_shouldMarkTimeout() {
        // given
        OrderModel order = createOrderedOrder();
        persistenceService.savePendingAndGetRequestParam(USER_ID, order.getId(), "SAMSUNG", "1", CB);
        when(pgSimulatorClient.getPaymentsByOrderId(order.getId())).thenReturn(null);

        // when
        paymentFacade.recoverPendingFromPgSimulator(order.getId());

        // then
        assertThat(orderService.findById(USER_ID, order.getId()).orElseThrow().getStatus()).isEqualTo(OrderStatus.ORDERED);
        assertThat(paymentRepository.findTopByOrderIdOrderByCreatedAtDesc(order.getId()).orElseThrow().getStatus())
                .isEqualTo(PaymentStatus.TIMEOUT);
    }

    /** PG success 필드가 비정상(null)인 경우도 무기한 PENDING을 막기 위해 TIMEOUT으로 수렴시킨다. */
    @Test
    @DisplayName("PG 조회의 success가 null이면 PENDING을 TIMEOUT으로 바꾼다.")
    void recoverOrPoll_whenPgSuccessIsNull_shouldMarkTimeout() {
        // given
        OrderModel order = createOrderedOrder();
        persistenceService.savePendingAndGetRequestParam(USER_ID, order.getId(), "SAMSUNG", "1", CB);
        when(pgSimulatorClient.getPaymentsByOrderId(order.getId()))
                .thenReturn(new PgPaymentStatusResponse("pg-poll-unknown", order.getId(), null, "UNKNOWN", null, null));

        // when
        paymentFacade.recoverPendingFromPgSimulator(order.getId());

        // then
        assertThat(orderService.findById(USER_ID, order.getId()).orElseThrow().getStatus()).isEqualTo(OrderStatus.ORDERED);
        assertThat(paymentRepository.findTopByOrderIdOrderByCreatedAtDesc(order.getId()).orElseThrow().getStatus())
                .isEqualTo(PaymentStatus.TIMEOUT);
    }

    /** 복구 호출 입력 검증. */
    @Test
    @DisplayName("orderId가 null이면 BAD_REQUEST다.")
    void recoverOrPoll_whenOrderIdNull_shouldThrowBadRequest() {
        CoreException ex = assertThrows(CoreException.class, () -> paymentFacade.recoverPendingFromPgSimulator(null));
        assertThat(ex.getErrorType()).isEqualTo(ErrorType.BAD_REQUEST);
    }

    /** PG 일시 장애 시 복구 스케줄/수동 호출이 죽지 않고 PENDING 유지(재폴링 가능). */
    @Test
    @DisplayName("PG 조회가 예외를 던져도 복구 호출은 전파하지 않는다.")
    void recoverOrPoll_whenPgThrows_shouldSwallowAndKeepPending() {
        // given
        OrderModel order = createOrderedOrder();
        persistenceService.savePendingAndGetRequestParam(USER_ID, order.getId(), "SAMSUNG", "1", CB);
        when(pgSimulatorClient.getPaymentsByOrderId(order.getId())).thenThrow(new RuntimeException("pg down"));

        // when / then
        assertThatCode(() -> paymentFacade.recoverPendingFromPgSimulator(order.getId())).doesNotThrowAnyException();
        assertThat(paymentRepository.findTopByOrderIdOrderByCreatedAtDesc(order.getId()).orElseThrow().getStatus())
                .isEqualTo(PaymentStatus.PENDING);
    }
}
