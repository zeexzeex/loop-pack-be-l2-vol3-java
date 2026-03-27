package com.loopers.collector.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.collector.idempotency.LightweightEventIdempotency;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProductEventCollectorServiceTest {

    @Mock
    private ProductEventCollectorDatabaseService databaseService;

    @Mock
    private LightweightEventIdempotency lightweightEventIdempotency;

    private SimpleMeterRegistry meterRegistry;
    private ProductEventCollectorService collectorService;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        collectorService = new ProductEventCollectorService(
                databaseService,
                lightweightEventIdempotency,
                new ObjectMapper().findAndRegisterModules(),
                meterRegistry
        );
    }

    @Test
    @DisplayName("PRODUCT_LIKE_CHANGED는 DB 트랜잭션 경로로 위임한다.")
    void process_whenLikeEvent_shouldDelegateToDatabaseService() {
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>(
                "product-events",
                0,
                5L,
                "101",
                envelopeJson("evt-1", "PRODUCT_LIKE_CHANGED", "2026-03-26T00:00:00Z", 101L, "LIKED").getBytes()
        );

        collectorService.process(record);

        verify(databaseService).processDb(any(ConsumerRecord.class), any());
        verify(lightweightEventIdempotency, never()).tryClaimFirstDelivery(any());
    }

    @Test
    @DisplayName("USER_REGISTERED는 Redis 멱등만 사용하고 DB event_handled 경로는 호출하지 않는다.")
    void process_whenUserRegistered_shouldUseLightweightIdempotencyOnly() {
        when(lightweightEventIdempotency.tryClaimFirstDelivery("evt-user-1")).thenReturn(true);
        String json = "{\"eventId\":\"evt-user-1\",\"eventType\":\"USER_REGISTERED\","
                + "\"occurredAt\":\"2026-03-26T00:00:00Z\",\"partitionKey\":\"1\","
                + "\"data\":{\"userId\":1,\"loginId\":\"u1\"}}";
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>(
                "user-events",
                0,
                1L,
                "1",
                json.getBytes()
        );

        collectorService.process(record);

        verify(lightweightEventIdempotency).tryClaimFirstDelivery("evt-user-1");
        verify(databaseService, never()).processDb(any(), any());
        assertThat(meterRegistry.find("kafka.collector.events.processed").counter().count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("USER_REGISTERED 중복 수신 시 Redis 멱등 실패로 DB 없이 종료한다.")
    void process_whenUserRegisteredDuplicate_shouldSkipWithoutDb() {
        when(lightweightEventIdempotency.tryClaimFirstDelivery("evt-user-dup")).thenReturn(false);
        String json = "{\"eventId\":\"evt-user-dup\",\"eventType\":\"USER_REGISTERED\","
                + "\"occurredAt\":\"2026-03-26T00:00:00Z\",\"partitionKey\":\"1\","
                + "\"data\":{\"userId\":1}}";
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("user-events", 0, 2L, "1", json.getBytes());

        collectorService.process(record);

        verify(databaseService, never()).processDb(any(), any());
        assertThat(meterRegistry.find("kafka.collector.events.duplicate").counter().count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("이벤트 파싱 실패 시 실패 메트릭을 증가시킨다.")
    void process_whenInvalidEnvelope_shouldIncreaseFailedMetric() {
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>(
                "product-events",
                0,
                1L,
                "101",
                "{not-json".getBytes()
        );

        try {
            collectorService.process(record);
        } catch (IllegalArgumentException ignored) {
        }

        assertThat(meterRegistry.find("kafka.collector.events.failed").counter().count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("envelope 최상위에 미정의 필드가 오면 역직렬화 실패로 예외가 발생한다.")
    void process_whenEnvelopeHasUnknownFields_shouldFailDeserialization() {
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>(
                "product-events",
                0,
                8L,
                "101",
                ("{"
                        + "\"eventId\":\"evt-unknown\","
                        + "\"eventType\":\"PRODUCT_LIKE_CHANGED\","
                        + "\"occurredAt\":\"2026-03-26T00:00:00Z\","
                        + "\"partitionKey\":\"101\","
                        + "\"schemaVersion\":\"v2\","
                        + "\"unknownTopLevel\":\"ignored\","
                        + "\"data\":{\"productId\":101,\"action\":\"LIKED\",\"futureField\":\"x\"}"
                        + "}").getBytes()
        );

        try {
            collectorService.process(record);
        } catch (IllegalArgumentException ignored) {
        }

        verify(databaseService, never()).processDb(any(), any());
        assertThat(meterRegistry.find("kafka.collector.events.failed").counter().count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("eventId가 비어 있으면 실패 메트릭을 증가시키고 예외를 던진다.")
    void process_whenEventIdMissing_shouldFail() {
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>(
                "product-events",
                0,
                9L,
                "101",
                ("{"
                        + "\"eventType\":\"PRODUCT_LIKE_CHANGED\","
                        + "\"occurredAt\":\"2026-03-26T00:00:00Z\","
                        + "\"partitionKey\":\"101\","
                        + "\"data\":{\"productId\":101,\"action\":\"LIKED\"}"
                        + "}").getBytes()
        );

        try {
            collectorService.process(record);
        } catch (IllegalArgumentException ignored) {
        }

        verify(databaseService, never()).processDb(any(), any());
        assertThat(meterRegistry.find("kafka.collector.events.failed").counter().count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("CART_ITEM_ADDED도 경량 멱등 경로를 사용한다.")
    void process_whenCartItemAdded_shouldUseLightweightPath() {
        when(lightweightEventIdempotency.tryClaimFirstDelivery("evt-cart-1")).thenReturn(true);
        String json = "{\"eventId\":\"evt-cart-1\",\"eventType\":\"CART_ITEM_ADDED\","
                + "\"occurredAt\":\"2026-03-26T00:00:00Z\",\"partitionKey\":\"1\","
                + "\"data\":{\"userId\":1,\"productId\":101,\"quantity\":2}}";
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("user-events", 0, 3L, "1", json.getBytes());

        collectorService.process(record);

        verify(lightweightEventIdempotency).tryClaimFirstDelivery("evt-cart-1");
        verify(databaseService, never()).processDb(any(), any());
        assertThat(meterRegistry.find("kafka.collector.events.processed").counter().count()).isEqualTo(1.0);
    }

    private static String envelopeJson(String eventId, String eventType, String occurredAt, Long productId, String action) {
        return "{"
                + "\"eventId\":\"" + eventId + "\","
                + "\"eventType\":\"" + eventType + "\","
                + "\"occurredAt\":\"" + occurredAt + "\","
                + "\"partitionKey\":\"" + productId + "\","
                + "\"data\":{"
                + "\"productId\":" + productId + ","
                + "\"action\":\"" + action + "\""
                + "}"
                + "}";
    }
}
