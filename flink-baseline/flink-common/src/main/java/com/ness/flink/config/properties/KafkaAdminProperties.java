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

import static com.ness.flink.config.properties.OperatorPropertiesFactory.DEFAULT_CONFIG_FILE;

import com.google.common.annotations.VisibleForTesting;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.kafka.clients.admin.AdminClientConfig;

@Slf4j
@Getter
@Setter
@ToString
public class KafkaAdminProperties extends KafkaProperties implements RawProperties<KafkaAdminProperties> {
    private static final long serialVersionUID = 8374164378532623386L;
    private static final String SHARED_PROPERTY_NAME = "kafka.admin";

    private String bootstrapServers;
    private Integer connectionMaxIdleMs;


    public static KafkaAdminProperties from(@NonNull String name, @NonNull ParameterTool parameterTool) {
        return from(name, parameterTool, DEFAULT_CONFIG_FILE);
    }

    @VisibleForTesting
    static KafkaAdminProperties from(@NonNull String name, @NonNull ParameterTool parameterTool,
        @NonNull String ymlConfigFile) {
        SharedKafkaProperties sharedProperties = SharedKafkaProperties.from(name, parameterTool, ymlConfigFile);
        KafkaAdminProperties adminProperties = OperatorPropertiesFactory
            .from(name, SHARED_PROPERTY_NAME, parameterTool, KafkaAdminProperties.class, ymlConfigFile);
        // Original Kafka Admin properties
        final Map<String, String> adminRawValues = new LinkedHashMap<>(adminProperties.rawValues);
        // Provide all default Kafka properties
        adminProperties.rawValues.putAll(sharedProperties.getRawValues());
        // Now overwrites with Admin related
        adminProperties.rawValues.putAll(adminRawValues);
        log.info("Building Kafka Admin: adminProperties={}", adminProperties);
        return adminProperties;
    }

    public Properties getAdminProperties() {
        Properties adminProperties = new Properties();
        Map<String, String> filtered = filterNonAdminProperties();
        // We should provide unique prefix (in our case it's Operator name) for building "client.id"
        filtered.putIfAbsent(KafkaSourceOptions.CLIENT_ID_PREFIX.key(), getName());
        adminProperties.putAll(filtered);
        log.info("Building Kafka AdminProperties: properties={}", adminProperties);
        return adminProperties;
    }

    private Map<String, String> filterNonAdminProperties() {
        return rawValues.entrySet().stream()
            .filter(e -> AdminClientConfig.configNames().contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

}
