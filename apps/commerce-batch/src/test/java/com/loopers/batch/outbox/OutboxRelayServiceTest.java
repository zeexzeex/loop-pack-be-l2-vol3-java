package com.loopers.batch.outbox;

import com.loopers.infrastructure.outbox.OutboxEventModel;
import com.loopers.infrastructure.outbox.OutboxJpaRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxRelayServiceTest {

    @Mock
    private OutboxJpaRepository outboxJpaRepository;

    @Mock
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Test
    @DisplayName("Kafka 전송 실패 시 published는 false로 유지되어 다음 폴링에서 재시도된다.")
    void relayOnce_whenSendFails_shouldKeepUnpublished() {
        OutboxEventModel event = OutboxEventModel.pending(
                "evt-fail",
                "product-events",
                "101",
                "PRODUCT_LIKE_CHANGED",
                Instant.parse("2026-03-27T00:00:00Z"),
                "{\"productId\":101,\"action\":\"LIKED\"}"
        );
        when(outboxJpaRepository.findPendingForUpdateSkipLocked(10)).thenReturn(List.of(event));
        when(kafkaTemplate.send(any(org.apache.kafka.clients.producer.ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        OutboxRelayService service = new OutboxRelayService(
                outboxJpaRepository,
                kafkaTemplate,
                new OutboxRelayProperties(true, 100, Duration.ofSeconds(1))
        );

        int relayed = service.relayOnce(10);

        assertThat(relayed).isEqualTo(1);
        assertThat(event.isPublished()).isFalse();
        assertThat(event.getPublishedAt()).isNull();
    }
}
