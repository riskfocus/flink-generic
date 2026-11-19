/*
 * Copyright 2021-2023 Ness Digital Engineering
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ness.flink.distribution;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.util.MathUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * This test shows how keyBy works on Apache Flink
 * <p>
 * Flink uses keyed streams to scale out to cluster nodes.
 * The concept involves the events of a stream being partitioned according to a specific key.
 * Flink then processes different partitions on different nodes.
 * </p>
 * <p>Keys are mapped to key groups</p>
 * <code>keyGroupId= MathUtils.murmurHash(key.hashCode()) % maxParallelism </code>
 * <p>Key groups are assigned to slots</p>
 * <code>slotIndex= keyGroupId * actualParallelism / maxParallelism</code>
 * @author Khokhlov Pavel
 */
@Slf4j
class FlinkKeyGroupTest {

    /**
     * Default Maximum parallelism
     */
    static final int MAX_PARALLELISM = 128;
    static final Random RND = new Random();
    /**
     * Number of randomly generates keys
     */
    static final int NUMBER_OF_KEYS = 1000;

    /**
     * You could play with different level of actualParallelism and
     * see how data will be distributed across task slots
     * @param actualParallelism actual parallelism on Flink cluster
     */
    @ParameterizedTest
    @ValueSource(ints = 4)
    void showDataDistributionFlinkKeyBy(int actualParallelism) {
        Map<Integer, Integer> distribution = new HashMap<>();
        IntStream.range(0, actualParallelism).forEach(i -> distribution.put(i, 0));
        keysProvider().forEach(key -> {
            final int slotIndex = getSlotIndexByKey(key, actualParallelism);
            distribution.compute(slotIndex, (slotIdx, currentValue) -> {
                if (currentValue == null) {
                    return 1;
                } else {
                    return currentValue + 1;
                }
            });
        });
        Assertions.assertEquals(actualParallelism, distribution.size());
        Map<Integer, Integer> sortedMap =
            distribution.entrySet().stream()
                .sorted(Entry.comparingByValue())
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                    (e1, e2) -> e1, LinkedHashMap::new));

        sortedMap.forEach((key, value) -> log.info("Keys: k={}, v={}", key, value));
    }

    int getSlotIndexByKey(String key, int actualParallelism) {
        int keyGroupId = MathUtils.murmurHash(key.hashCode()) % MAX_PARALLELISM;
        return keyGroupId * actualParallelism / MAX_PARALLELISM;
    }

    Stream<String> keysProvider() {
        return IntStream.range(0, NUMBER_OF_KEYS).mapToObj(value -> String.valueOf(RND.nextInt() + value));
    }
}
