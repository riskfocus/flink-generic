/*
 * Copyright 2021-2024 Ness Digital Engineering
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.ness.flink.dsl.kafka.properties;

import com.ness.flink.dsl.properties.PropertiesProvider;
import com.ness.flink.dsl.properties.RawProperties;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * Encapsulates Kafka-specific Flink source properties, both operator level, such as {@code name}, and Kafka Consumer
 * level, such as {@code group.id} etc. Each source might have:
 * <ul>
 *     <li>shared Kafka properties, defined with prefix {@code kafka}</li>
 *     <li>shared Kafka consumer properties, defined with prefix {@code kafka.consumer}</li>
 *     <li>its own properties, defined with operator name prefix</li>
 * </ul>
 * Note that precedence is the same as the order above.
 */
@Data
@NoArgsConstructor
@Slf4j
public class KafkaSourceProperties implements RawProperties<KafkaSourceProperties> {

    private static final String KAFKA_PREFIX = "kafka";
    private static final String CONSUMER_PREFIX = "kafka.consumer";
    private static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

    @ToString.Exclude
    private final Map<String, String> rawValues = new HashMap<>();

    @Nonnull
    private String name;
    @Nonnull
    private Integer parallelism;
    @Nonnull
    private String topic;
    private Long timestamp;
    private boolean skipBrokenMessages;

    private Offsets offsets = Offsets.EARLIEST;
    private OffsetResetStrategy offsetResetStrategy = OffsetResetStrategy.EARLIEST;

    public static KafkaSourceProperties from(@Nonnull String name, @Nonnull PropertiesProvider propertiesProvider) {
        KafkaSourceProperties properties = propertiesProvider.namedProperties(name,
            KafkaSourceProperties.class, KAFKA_PREFIX, CONSUMER_PREFIX);
        log.debug("Kafka source properties: value={}", properties);

        return properties;
    }

    @Override
    public KafkaSourceProperties withRawValues(@Nonnull Map<String, String> defaults) {
        rawValues.putAll(defaults);

        return this;
    }

    public Properties asJavaProperties() {
        Properties consumerProperties = new Properties();
        Map<String, String> filtered = filterNonConsumerProperties();
        consumerProperties.putAll(filtered);

        return consumerProperties;
    }

    /**
     * Starting/stopping offset for Kafka source.
     */
    public OffsetsInitializer offsetsInitializer() {
        switch (offsets) {
            case LATEST:
                return OffsetsInitializer.latest();
            case EARLIEST:
                return OffsetsInitializer.earliest();
            case TIMESTAMP:
                return OffsetsInitializer.timestamp(getTimestamp());
            case COMMITTED:
                return OffsetsInitializer.committedOffsets(offsetResetStrategy);
            default:
                throw new IllegalArgumentException("Unsupported offsets value: " + offsets);
        }
    }

    private Map<String, String> filterNonConsumerProperties() {
        return rawValues.entrySet().stream()
            .filter(e -> ConsumerConfig.configNames().contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public String schemaRegistryUrl() {
        return rawValues.get(SCHEMA_REGISTRY_URL);
    }

    enum Offsets {
        EARLIEST, LATEST, TIMESTAMP, COMMITTED
    }

}