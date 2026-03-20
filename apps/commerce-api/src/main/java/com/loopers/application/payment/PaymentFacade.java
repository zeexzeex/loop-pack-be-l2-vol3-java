package com.loopers.application.payment;

import com.loopers.domain.order.OrderModel;
import com.loopers.domain.order.OrderService;
import com.loopers.domain.order.OrderRepository;
import com.loopers.domain.payment.PaymentModel;
import com.loopers.domain.payment.PaymentRepository;
import com.loopers.infrastructure.payment.PgPaymentStatusResponse;
import com.loopers.infrastructure.payment.PgSimulatorClient;
import com.loopers.infrastructure.payment.PgSimulatorRequest;
import com.loopers.support.error.CoreException;
import com.loopers.support.error.ErrorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.RoundingMode;

/**
 * 결제 유스케이스 조율 (06 §10.2).
 * (1) DB 트랜잭션으로 PENDING 저장 (2) 트랜잭션 종료 후 PG 호출.
 * (3) Phase 3: 콜백 처리 handleCallback.
 * (4) Phase 8: PG 조회 API로 PENDING 동기화 {@link #recoverPendingFromPgSimulator(Long)}.
 */
@Service
public class PaymentFacade {

    private static final Logger log = LoggerFactory.getLogger(PaymentFacade.class);

    private final PaymentPersistenceService persistenceService;
    private final PgPaymentRequester pgPaymentRequester;
    private final PgSimulatorClient pgSimulatorClient;
    private final ObjectProvider<PaymentFacade> paymentFacadeSelf;
    private final String callbackUrl;
    private final String callbackSecret;
    private final OrderService orderService;
    private final OrderRepository orderRepository;
    private final PaymentRepository paymentRepository;

    public PaymentFacade(PaymentPersistenceService persistenceService,
                         @Value("${pg.simulator.callback-url}") String callbackUrl,
                         @Value("${pg.simulator.callback-secret:}") String callbackSecret,
                         OrderService orderService,
                         OrderRepository orderRepository,
                         PaymentRepository paymentRepository,
                         PgPaymentRequester pgPaymentRequester,
                         PgSimulatorClient pgSimulatorClient,
                         ObjectProvider<PaymentFacade> paymentFacadeSelf) {
        this.persistenceService = persistenceService;
        this.pgPaymentRequester = pgPaymentRequester;
        this.pgSimulatorClient = pgSimulatorClient;
        this.paymentFacadeSelf = paymentFacadeSelf;
        this.callbackUrl = callbackUrl;
        this.callbackSecret = callbackSecret != null ? callbackSecret : "";
        this.orderService = orderService;
        this.orderRepository = orderRepository;
        this.paymentRepository = paymentRepository;
    }

    /**
     * 콜백 발신 주체 검증 (06-payment-change-issues §4.1). 시크릿이 설정된 경우에만 검사.
     */
    public void verifyCallbackSecret(String headerSecret) {
        if (callbackSecret.isEmpty()) {
            return;
        }
        if (headerSecret == null || !callbackSecret.equals(headerSecret)) {
            throw new CoreException(ErrorType.UNAUTHORIZED, "콜백 인증에 실패했습니다.");
        }
    }

    /**
     * 결제 요청. Phase 2: PENDING 저장 후 트랜잭션 밖에서 PG 호출.
     * Retry/CB/Fallback은 Phase 4~6에서 적용.
     */
    public PaymentInfo requestPayment(Long userId, Long orderId, String cardType, String cardNo) {
        PendingPaymentResult result = persistenceService.savePendingAndGetRequestParam(
                userId, orderId, cardType, cardNo, callbackUrl);
        // 트랜잭션 밖: PG 호출 (06 §5.1). 실패 시에도 200 + PENDING 반환 (06-payment-change-issues §3.1).
        PaymentRequestParam param = result.requestParam();
        PgSimulatorRequest request = new PgSimulatorRequest(
                param.orderId(),
                param.cardType(),
                param.cardNo(),
                param.amount(),
                param.callbackUrl()
        );
        try {
            pgPaymentRequester.requestPaymentToPg(request);
        } catch (Exception e) {
            // PENDING 저장은 이미 커밋됨. PG 타임아웃/5xx 시에도 200 + PENDING으로 응답해 UX·재시도 일관성 유지.
        }
        return result.paymentInfo();
    }

    /**
     * PG 콜백 처리 (06 §3, §9). PENDING 결제를 먼저 조회한 뒤 처리 (06-payment-change-issues §3.2).
     * 없으면 이미 처리된 건으로 멱등 반환.
     */
    @Transactional
    public void handleCallback(PaymentCallbackParam param) {
        var pendingOpt = paymentRepository.findTopByOrderIdOrderByCreatedAtDesc(param.orderId())
                .filter(PaymentModel::isPending);

        if (pendingOpt.isEmpty()) {
            return;
        }
        PaymentModel payment = pendingOpt.get();

        if (param.success()) {
            if (param.amount() != null) {
                OrderModel order = orderRepository.findById(param.orderId())
                        .orElse(null);
                if (order != null) {
                    long orderAmountWon = order.getFinalAmount().setScale(0, RoundingMode.HALF_UP).longValue();
                    if (param.amount() != orderAmountWon) {
                        log.warn("콜백 금액 불일치 orderId={} pgAmount={} orderAmount={}", param.orderId(), param.amount(), orderAmountWon);
                        payment.markFailed();
                        paymentRepository.save(payment);
                        return;
                    }
                }
            }
            orderService.completePayment(param.orderId());
            payment.markSuccess(param.pgTransactionId());
            paymentRepository.save(payment);
        } else {
            payment.markFailed();
            paymentRepository.save(payment);
        }
    }

    /**
     * 콜백 미수신 시 PG 주문별 조회로 PENDING 건을 동기화한다 (06 §11.3~11.4, Phase 8).
     * {@link #handleCallback(PaymentCallbackParam)}는 프록시를 통해 호출되어 트랜잭션이 적용된다.
     */
    public void recoverPendingFromPgSimulator(Long orderId) {
        if (orderId == null) {
            throw new CoreException(ErrorType.BAD_REQUEST, "orderId는 필수입니다.");
        }
        boolean hasPending = paymentRepository.findTopByOrderIdOrderByCreatedAtDesc(orderId)
                .filter(PaymentModel::isPending)
                .isPresent();
        if (!hasPending) {
            return;
        }
        PgPaymentStatusResponse pg;
        try {
            pg = pgSimulatorClient.getPaymentsByOrderId(orderId);
        } catch (Exception e) {
            log.warn("PG 주문별 조회 실패 orderId={}", orderId, e);
            return;
        }
        if (pg == null) {
            log.warn("PG 주문별 조회 응답이 비어 있음 orderId={}, PENDING을 TIMEOUT 처리", orderId);
            paymentFacadeSelf.getObject().timeoutPendingPaymentForOrder(orderId);
            return;
        }
        Long amountForCallback = pg.amount();
        if (amountForCallback == null) {
            amountForCallback = orderRepository.findById(orderId)
                    .map(o -> o.getFinalAmount().setScale(0, RoundingMode.HALF_UP).longValue())
                    .orElse(null);
        }
        PaymentFacade facade = paymentFacadeSelf.getObject();
        if (Boolean.TRUE.equals(pg.success())) {
            facade.handleCallback(new PaymentCallbackParam(
                    orderId, true, pg.paymentId(), pg.failureReason(), amountForCallback));
        } else if (Boolean.FALSE.equals(pg.success())) {
            facade.handleCallback(new PaymentCallbackParam(
                    orderId, false, pg.paymentId(), pg.failureReason(), pg.amount()));
        } else {
            log.warn("PG 주문별 조회 success 값 비정상(null) orderId={}, PENDING을 TIMEOUT 처리", orderId);
            paymentFacadeSelf.getObject().timeoutPendingPaymentForOrder(orderId);
        }
    }

    /**
     * PG에 결제 기록이 없을 때(응답 null 등) PENDING을 TIMEOUT으로 정리한다 (06 §11.4 미접수 근사).
     */
    @Transactional
    public void timeoutPendingPaymentForOrder(Long orderId) {
        paymentRepository.findTopByOrderIdOrderByCreatedAtDesc(orderId)
                .filter(PaymentModel::isPending)
                .ifPresent(p -> {
                    p.markTimeout();
                    paymentRepository.save(p);
                });
    }
}
