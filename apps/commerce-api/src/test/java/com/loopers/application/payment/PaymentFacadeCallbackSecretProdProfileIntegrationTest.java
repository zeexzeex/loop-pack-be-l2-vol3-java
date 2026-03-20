package com.loopers.application.payment;

import com.loopers.domain.order.OrderRepository;
import com.loopers.domain.order.OrderService;
import com.loopers.domain.payment.PaymentRepository;
import com.loopers.infrastructure.payment.PgSimulatorClient;
import com.loopers.support.error.CoreException;
import com.loopers.support.error.ErrorType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.env.Environment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 운영 프로필에서는 callback-secret 미설정을 허용하지 않는다.
 */
class PaymentFacadeCallbackSecretProdProfileIntegrationTest {

    @Test
    @DisplayName("운영 프로필에서 callback-secret이 비어 있으면 콜백을 거부한다.")
    void verifyCallbackSecret_whenProdAndSecretEmpty_shouldThrowUnauthorized() {
        Environment environment = mock(Environment.class);
        when(environment.getActiveProfiles()).thenReturn(new String[]{"prd"});

        PaymentFacade paymentFacade = new PaymentFacade(
                mock(PaymentPersistenceService.class),
                "http://localhost:8080/api/v1/payments/callback",
                "",
                mock(OrderService.class),
                mock(OrderRepository.class),
                mock(PaymentRepository.class),
                mock(PgPaymentRequester.class),
                mock(PgSimulatorClient.class),
                mock(ObjectProvider.class),
                environment
        );

        CoreException ex = assertThrows(CoreException.class,
                () -> paymentFacade.verifyCallbackSecret("anything"));
        assertThat(ex.getErrorType()).isEqualTo(ErrorType.UNAUTHORIZED);
    }
}
