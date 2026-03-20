package com.loopers.application.payment;

import com.loopers.domain.order.OrderRepository;
import com.loopers.domain.order.OrderService;
import com.loopers.domain.payment.PaymentRepository;
import com.loopers.infrastructure.payment.PgSimulatorClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

/**
 * {@link PaymentFacade#verifyCallbackSecret(String)}는 설정값이 비어 있으면 검증을 생략한다.
 * 운영에서도 시크릿 미설정 시 동일하며, 보안은 {@code pg.simulator.callback-secret} 설정 규율에 맡긴다.
 */
class PaymentFacadeCallbackSecretProdProfileIntegrationTest {

    @SuppressWarnings("unchecked")
    private static PaymentFacade newFacadeWithSecret(String callbackSecret) {
        return new PaymentFacade(
                mock(PaymentPersistenceService.class),
                "http://localhost:8080/api/v1/payments/callback",
                callbackSecret,
                mock(OrderService.class),
                mock(OrderRepository.class),
                mock(PaymentRepository.class),
                mock(PgPaymentRequester.class),
                mock(PgSimulatorClient.class),
                mock(ObjectProvider.class));
    }

    @Test
    @DisplayName("callback-secret이 비어 있으면 헤더 없이도 콜백 검증을 통과한다.")
    void verifyCallbackSecret_whenSecretEmpty_shouldNotThrow() {
        PaymentFacade paymentFacade = newFacadeWithSecret("");
        assertDoesNotThrow(() -> paymentFacade.verifyCallbackSecret(null));
        assertDoesNotThrow(() -> paymentFacade.verifyCallbackSecret("any"));
    }
}
