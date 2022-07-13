/*
 * Copyright 2020-2022 Ness USA, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ness.flink.dsl.properties;

import com.ness.flink.dsl.definition.kafka.CommonKafkaSource;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Kafka consumer properties for {@link CommonKafkaSource},created by {@link OperatorPropertiesFactory}.
 * Sets {@code group.id} equals to operator's name, if absent.
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaConsumerProperties implements RawProperties<KafkaConsumerProperties> {

    @Getter
    private final String name;
    @Getter
    private final String topic;
    @Getter
    @Nullable
    private final Integer parallelism;
    private final Map<String, String> rawValues = new HashMap<>();

    /**
     * @inheritDoc
     */
    @SneakyThrows
    public static KafkaConsumerProperties from(String name, ParameterTool params) {
        KafkaConsumerProperties properties = OperatorPropertiesFactory.from(name, params, KafkaConsumerProperties.class);
        properties.rawValues.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, name);

        return properties;
    }

    /**
     * @inheritDoc
     */
    @Override
    public KafkaConsumerProperties withRawValues(Map<String, String> defaults) {
        rawValues.putAll(defaults);

        return this;
    }

    /**
     * Returns all properties as {@link Properties} instance, for Kafka consumer configuration.
     * Perform property names filtering
     */
    public Properties getConsumerProperties() {
        Properties properties = new Properties();
        properties.putAll(filterNonConsumerParameters());

        return properties;
    }

    private Map<String, String> filterNonConsumerParameters() {
        return rawValues.entrySet().stream()
                .filter(e -> ConsumerConfig.configNames().contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

}
