/*
 * Copyright 2020 Risk Focus Inc
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

package com.riskfocus.dsl.properties;

import com.riskfocus.dsl.definition.kafka.CommonKafkaSource;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Kafka producer properties for {@link CommonKafkaSource}, created by {@link OperatorPropertiesFactory}
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaProducerProperties implements RawProperties<KafkaProducerProperties> {

    @Getter
    private final String name;
    @Getter
    private final String topic;
    @Getter
    @Nullable
    private final Integer parallelism;
    @Getter
    private final FlinkKafkaProducer.Semantic semantic;
    private final Map<String, String> rawValues = new HashMap<>();

    /**
     * @inheritDoc
     */
    @SneakyThrows
    public static KafkaProducerProperties from(String name, ParameterTool params) {
        return OperatorPropertiesFactory.from(name, params, KafkaProducerProperties.class);
    }

    /**
     * @inheritDoc
     */
    @Override
    public KafkaProducerProperties withRawValues(Map<String, String> defaults) {
        rawValues.putAll(defaults);

        return this;
    }

    /**
     * Returns all properties as {@link Properties} instance, for Kafka producer configuration.
     * Perform property names filtering
     */
    public Properties getProducerProperties() {
        Properties properties = new Properties();
        properties.putAll(filterNonProducerParameters());

        return properties;
    }

    private Map<String, String> filterNonProducerParameters() {
        return rawValues.entrySet().stream()
                .filter(e -> ProducerConfig.configNames().contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

}
