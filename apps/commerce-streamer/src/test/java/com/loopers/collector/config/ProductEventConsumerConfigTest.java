package com.loopers.collector.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class ProductEventConsumerConfigTest {

    @Test
    @DisplayName("collector listener factory는 auto-commit false와 MANUAL ack를 강제한다.")
    void productEventListenerFactory_shouldUseManualAckAndDisableAutoCommit() {
        KafkaProperties properties = new KafkaProperties();

        ProductEventConsumerConfig config = new ProductEventConsumerConfig();
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                config.productEventListenerContainerFactory(
                        properties,
                        mock(KafkaTemplate.class),
                        ".DLQ"
                );

        assertThat(factory.getContainerProperties().getAckMode()).isEqualTo(ContainerProperties.AckMode.MANUAL);
        assertThat(factory.isBatchListener()).isFalse();

        @SuppressWarnings("unchecked")
        DefaultKafkaConsumerFactory<Object, Object> cf =
                (DefaultKafkaConsumerFactory<Object, Object>) factory.getConsumerFactory();
        Map<String, Object> consumerConfig = cf.getConfigurationProperties();
        assertThat(consumerConfig.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo(false);
        assertThat(consumerConfig.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)).isEqualTo("earliest");
    }
}
