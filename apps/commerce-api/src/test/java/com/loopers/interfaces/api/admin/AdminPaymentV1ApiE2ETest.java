package com.loopers.interfaces.api.admin;

import com.loopers.application.payment.PaymentPersistenceService;
import com.loopers.domain.brand.BrandService;
import com.loopers.domain.order.OrderModel;
import com.loopers.domain.order.OrderService;
import com.loopers.domain.order.OrderStatus;
import com.loopers.domain.product.ProductModel;
import com.loopers.domain.product.ProductService;
import com.loopers.domain.product.ProductValidationRequest;
import com.loopers.domain.product.Quantity;
import com.loopers.infrastructure.payment.PgPaymentStatusResponse;
import com.loopers.infrastructure.payment.PgSimulatorClient;
import com.loopers.interfaces.api.ApiResponse;
import com.loopers.support.auth.AdminAuthInterceptor;
import com.loopers.testcontainers.MySqlTestContainersConfig;
import com.loopers.utils.DatabaseCleanUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static com.loopers.interfaces.api.ApiResponse.Metadata.Result;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * 어드민 결제 복구 API E2E (06 Phase 8).
 * REST 엔드포인트 미구현 시까지 비활성화. 복구는 Facade·배치로 수행.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(MySqlTestContainersConfig.class)
@Disabled("어드민 결제 복구 REST 미구현 — 엔드포인트 추가 후 해제")
class AdminPaymentV1ApiE2ETest {

    private static final long USER_ID = 1L;
    private static final String CB = "http://localhost:8080/api/v1/payments/callback";
    private static final String LDAP_ID = "admin-payment-e2e";

    @Autowired
    private TestRestTemplate testRestTemplate;
    @Autowired
    private DatabaseCleanUp databaseCleanUp;
    @Autowired
    private OrderService orderService;
    @Autowired
    private BrandService brandService;
    @Autowired
    private ProductService productService;
    @Autowired
    private PaymentPersistenceService persistenceService;

    @Value("${loopers.admin.auth.signing-secret:change-me-in-production}")
    private String signingSecret;

    @MockBean
    private PgSimulatorClient pgSimulatorClient;

    @AfterEach
    void tearDown() {
        databaseCleanUp.truncateAllTables();
    }

    private HttpHeaders adminHeaders() {
        String signature = AdminAuthInterceptor.sign(LDAP_ID, signingSecret);
        HttpHeaders h = new HttpHeaders();
        h.set(AdminAuthInterceptor.HEADER_LDAP, LDAP_ID);
        h.set(AdminAuthInterceptor.HEADER_ADMIN_SIGNATURE, signature);
        return h;
    }

    private OrderModel createOrderedOrder() {
        Long brandId = brandService.registerBrand("admin-pay-brand").getId();
        ProductModel product = productService.registerProduct(brandId, "admin-pay-p", new BigDecimal("11000"), 5);
        return orderService.create(USER_ID, List.of(
                new ProductValidationRequest(product.getId(), Quantity.of(1), null)));
    }

    @Test
    @DisplayName("POST recover: 유효한 어드민 인증으로 PENDING을 PG 성공으로 동기화하면 200·주문 PAID")
    void recover_withValidAuth_shouldSyncFromPg() {
        OrderModel order = createOrderedOrder();
        persistenceService.savePendingAndGetRequestParam(USER_ID, order.getId(), "SAMSUNG", "1", CB);
        long amountWon = order.getFinalAmount().setScale(0, java.math.RoundingMode.HALF_UP).longValue();
        when(pgSimulatorClient.getPaymentsByOrderId(order.getId()))
                .thenReturn(new PgPaymentStatusResponse("e2e-rec", order.getId(), true, "OK", amountWon, null));

        ResponseEntity<ApiResponse<Object>> res = testRestTemplate.exchange(
                "/api-admin/v1/payments/" + order.getId() + "/recover",
                HttpMethod.POST,
                new HttpEntity<>(adminHeaders()),
                new ParameterizedTypeReference<>() {});

        assertAll(
                () -> assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK),
                () -> assertThat(res.getBody()).isNotNull(),
                () -> assertThat(res.getBody().meta().result()).isEqualTo(Result.SUCCESS),
                () -> assertThat(orderService.findById(USER_ID, order.getId()).orElseThrow().getStatus())
                        .isEqualTo(OrderStatus.PAID)
        );
    }

    @Test
    @DisplayName("GET pending: PENDING 건이 있으면 목록에 포함된다.")
    void listPending_withValidAuth_shouldReturnPending() {
        OrderModel order = createOrderedOrder();
        persistenceService.savePendingAndGetRequestParam(USER_ID, order.getId(), "SAMSUNG", "1", CB);

        ResponseEntity<ApiResponse<List<Map<String, Object>>>> res = testRestTemplate.exchange(
                "/api-admin/v1/payments/pending",
                HttpMethod.GET,
                new HttpEntity<>(adminHeaders()),
                new ParameterizedTypeReference<>() {});

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().data()).isNotEmpty();
        assertThat(res.getBody().data().get(0).get("orderId")).isEqualTo(order.getId().intValue());
        assertThat(res.getBody().meta().result()).isEqualTo(Result.SUCCESS);
    }

    @Test
    @DisplayName("인증 없이 recover 호출 시 401")
    void recover_withoutAuth_shouldReturn401() {
        ResponseEntity<ApiResponse<Object>> res = testRestTemplate.exchange(
                "/api-admin/v1/payments/1/recover",
                HttpMethod.POST,
                new HttpEntity<>(null),
                new ParameterizedTypeReference<>() {});

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }
}
