//Copyright 2021-2023 Ness Digital Engineering
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.ness.flink.config.properties;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Provides Kafka Producer properties
 *
 * @author Khokhlov Pavel
 */
@Slf4j
@Getter
@Setter
@ToString
public class KafkaProducerProperties extends KafkaProperties implements RawProperties<KafkaProducerProperties> {
    private static final long serialVersionUID = -6867523426958957197L;
    private static final String SHARED_PROPERTY_NAME = "kafka.producer";

    private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE;
    private String transactionIdPrefix;

    public static KafkaProducerProperties from(@NonNull String name, @NonNull ParameterTool parameterTool) {
        return from(name, parameterTool, OperatorPropertiesFactory.DEFAULT_CONFIG_FILE);
    }

    @VisibleForTesting
    static KafkaProducerProperties from(@NonNull String name, @NonNull ParameterTool parameterTool,
                                        @NonNull String ymlConfigFile) {
        SharedKafkaProperties sharedProperties = SharedKafkaProperties.from(name, parameterTool, ymlConfigFile);
        KafkaProducerProperties producerProperties = OperatorPropertiesFactory
                .from(name, SHARED_PROPERTY_NAME, parameterTool, KafkaProducerProperties.class, ymlConfigFile);
        // Save original Producer properties
        final Map<String, String> producerRawValues = new LinkedHashMap<>(producerProperties.rawValues);
        // Provide all default Kafka properties
        producerProperties.rawValues.putAll(sharedProperties.getRawValues());
        // Now overwrites with Producer properties
        producerProperties.rawValues.putAll(producerRawValues);
        log.info("Building Kafka Producer: producerProperties={}", producerProperties);
        return producerProperties;
    }

    public Properties getProducerProperties() {
        Properties producerProperties = new Properties();
        producerProperties.putAll(filterNonProducerProperties());
        log.info("Building Kafka ProducerProperties: producerProperties={}", producerProperties);
        return producerProperties;
    }

    private Map<String, String> filterNonProducerProperties() {
        return rawValues.entrySet().stream()
                .filter(e -> ProducerConfig.configNames().contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public Optional<String> buildTransactionIdPrefix(String sinkName) {
        if (transactionIdPrefix == null) {
            return Optional.empty();
        } else {
            return Optional.of(transactionIdPrefix + sinkName);
        }
    }

    @Override
    public KafkaProducerProperties withRawValues(Map<String, String> defaults) {
        rawValues.putAll(defaults);
        return this;
    }

}
