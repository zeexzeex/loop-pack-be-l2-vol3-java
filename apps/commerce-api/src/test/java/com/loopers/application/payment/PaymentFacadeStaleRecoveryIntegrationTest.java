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
import org.springframework.test.context.TestPropertySource;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link PaymentFacade#recoverPendingFromPgSimulator(Long)} 기반 복구 시나리오 검증.
 */
@SpringBootTest
@Import(MySqlTestContainersConfig.class)
@TestPropertySource(properties = "payment.recovery.pending-min-age=0s")
class PaymentFacadeStaleRecoveryIntegrationTest {

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
        Long brandId = brandService.registerBrand("stale-recovery-brand").getId();
        ProductModel product = productService.registerProduct(brandId, "stale-p", new BigDecimal("7000"), 9);
        return orderService.create(USER_ID, List.of(
                new ProductValidationRequest(product.getId(), Quantity.of(1), null)));
    }

    @Test
    @DisplayName("PENDING 복구 시 PG SUCCESS면 주문이 PAID가 된다.")
    void recoverPending_whenPgSuccess_shouldCompletePayment() {
        OrderModel order = createOrderedOrder();
        persistenceService.savePendingAndGetRequestParam(USER_ID, order.getId(), "SAMSUNG", "1", CB);
        long amountWon = order.getFinalAmount().setScale(0, java.math.RoundingMode.HALF_UP).longValue();
        when(pgSimulatorClient.getPaymentsByOrderId(order.getId()))
                .thenReturn(new PgPaymentStatusResponse("pg-stale-1", order.getId(), true, "OK", amountWon, null));

        paymentFacade.recoverPendingFromPgSimulator(order.getId());

        assertThat(orderService.findById(USER_ID, order.getId()).orElseThrow().getStatus()).isEqualTo(OrderStatus.PAID);
        assertThat(paymentRepository.findTopByOrderIdOrderByCreatedAtDesc(order.getId()).orElseThrow().getStatus())
                .isEqualTo(PaymentStatus.SUCCESS);
    }

    @Test
    @DisplayName("PENDING이 없으면 복구가 PG를 호출하지 않는다.")
    void recoverPending_whenNoPending_shouldNotCallPg() {
        OrderModel order = createOrderedOrder();

        paymentFacade.recoverPendingFromPgSimulator(order.getId());

        verify(pgSimulatorClient, never()).getPaymentsByOrderId(anyLong());
    }
}
