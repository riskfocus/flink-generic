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

package com.ness.flink.config.properties;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.ness.flink.config.properties.OperatorPropertiesFactory.DEFAULT_CONFIG_FILE;


/**
 * Provides Kafka Consumer properties
 *
 * @author Khokhlov Pavel
 */
@Slf4j
@Getter
@Setter
@ToString
public class KafkaConsumerProperties extends KafkaProperties implements RawProperties<KafkaConsumerProperties> {
    private static final long serialVersionUID = 8374164378532623386L;

    private static final String CONFLUENT_SCHEMA_REGISTRY_KEY = "schema.registry.url";
    private static final String SHARED_PROPERTY_NAME = "kafka.consumer";

    private Long timestamp;

    private boolean skipBrokenMessages;

    private Integer maxParallelism;

    private Offsets offsets = Offsets.EARLIEST;
    private OffsetResetStrategy offsetResetStrategy = OffsetResetStrategy.EARLIEST;

    public static KafkaConsumerProperties from(@NonNull String name, @NonNull ParameterTool parameterTool) {
        return from(name, parameterTool, DEFAULT_CONFIG_FILE);
    }

    @VisibleForTesting
    static KafkaConsumerProperties from(@NonNull String name, @NonNull ParameterTool parameterTool,
                                        @NonNull String ymlConfigFile) {
        SharedKafkaProperties sharedProperties = SharedKafkaProperties.from(name, parameterTool, ymlConfigFile);
        KafkaConsumerProperties consumerProperties = OperatorPropertiesFactory
                .from(name, SHARED_PROPERTY_NAME, parameterTool, KafkaConsumerProperties.class, ymlConfigFile);
        // Original Kafka Consumer properties
        final Map<String, String> consumerRawValues = new LinkedHashMap<>(consumerProperties.rawValues);
        // Provide all default Kafka properties
        consumerProperties.rawValues.putAll(sharedProperties.getRawValues());
        // Now overwrites with Consumer related
        consumerProperties.rawValues.putAll(consumerRawValues);
        // Register group.id if it wasn't registered
        consumerProperties.rawValues.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, name);
        log.info("Building Kafka Consumer: consumerProperties={}", consumerProperties);
        return consumerProperties;
    }
    
    public enum Offsets {
        EARLIEST, LATEST, TIMESTAMP, COMMITTED
    }

    public Properties getConsumerProperties() {
        Properties consumerProperties = new Properties();
        Map<String, String> filtered = filterNonConsumerProperties();
        // We should provide unique prefix (in our case it's Operator name) for building "client.id"
        filtered.putIfAbsent(KafkaSourceOptions.CLIENT_ID_PREFIX.key(), getName());
        consumerProperties.putAll(filtered);
        log.info("Building Kafka ConsumerProperties: properties={}", consumerProperties);
        return consumerProperties;
    }

    /**
     * Builds the starting / stopping offset for Kafka Consumer
     * @return an OffsetsInitializer which initializes the offsets.
     */
    public OffsetsInitializer getOffsetsInitializer() {
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
                throw new IllegalArgumentException("Unsupported value: " + offsets);
        }
    }

    private Map<String, String> filterNonConsumerProperties() {
        return rawValues.entrySet().stream()
                .filter(e -> ConsumerConfig.configNames().contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public KafkaConsumerProperties withRawValues(Map<String, String> defaults) {
        rawValues.putAll(defaults);
        return this;
    }
}
