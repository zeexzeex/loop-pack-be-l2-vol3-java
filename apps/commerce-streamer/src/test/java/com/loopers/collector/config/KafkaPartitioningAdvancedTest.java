package com.loopers.collector.config;

import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaPartitioningAdvancedTest {

    @Test
    @DisplayName("같은 key는 같은 파티션 수에서 항상 같은 파티션으로 매핑된다.")
    void sameKey_shouldMapToSamePartitionWhenPartitionCountSame() {
        String key = "order-1351039135";
        int partitionCount = 6;

        int p1 = partitionOf(key, partitionCount);
        int p2 = partitionOf(key, partitionCount);

        assertThat(p1).isEqualTo(p2);
    }

    @Test
    @DisplayName("파티션 수가 바뀌면 같은 key의 매핑 파티션이 달라질 수 있다.")
    void sameKey_canMoveWhenPartitionCountChanges() {
        String movedKey = null;
        int oldPartition = -1;
        int newPartition = -1;
        for (int i = 1; i <= 5000; i++) {
            String key = "product-" + i;
            int p3 = partitionOf(key, 3);
            int p6 = partitionOf(key, 6);
            if (p3 != p6) {
                movedKey = key;
                oldPartition = p3;
                newPartition = p6;
                break;
            }
        }

        assertThat(movedKey).isNotNull();
        assertThat(oldPartition).isNotEqualTo(newPartition);
    }

    private static int partitionOf(String key, int partitions) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        return Utils.toPositive(Utils.murmur2(keyBytes)) % partitions;
    }
}
