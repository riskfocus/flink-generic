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

package com.ness.flink.canary.pipeline.config.properties;

import com.google.common.annotations.VisibleForTesting;
import com.ness.flink.config.properties.KafkaConsumerProperties;
import com.ness.flink.config.properties.KafkaProperties;
import com.ness.flink.config.properties.OperatorPropertiesFactory;
import com.ness.flink.config.properties.RawProperties;
import com.ness.flink.config.properties.SharedKafkaProperties;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.ness.flink.config.properties.OperatorPropertiesFactory.DEFAULT_CONFIG_FILE;

@Slf4j
public class KafkaAdminProperties extends KafkaProperties implements RawProperties<KafkaAdminProperties> {
    private static final long serialVersionUID = 8374164378532623386L;
    private static final String SHARED_PROPERTY_NAME = "kafka.admin";

    private Integer connectionMaxIdleMs;
    private String bootstrapServers;

    public static KafkaAdminProperties from(@NonNull String name, @NonNull ParameterTool parameterTool) {
        return from(name, parameterTool, DEFAULT_CONFIG_FILE);
    }

    @VisibleForTesting
    static KafkaAdminProperties from(@NonNull String name, @NonNull ParameterTool parameterTool,
        @NonNull String ymlConfigFile) {
        // TODO Not sure how to fix this error here, I added KafkaAdminProperties to flink-common folder.
        SharedKafkaProperties sharedProperties = SharedKafkaProperties.from(name, parameterTool, ymlConfigFile);
        KafkaAdminProperties adminProperties = OperatorPropertiesFactory
            .from(name, SHARED_PROPERTY_NAME, parameterTool, KafkaAdminProperties.class, ymlConfigFile);
        // Original Kafka Consumer properties
        final Map<String, String> adminRawValues = new LinkedHashMap<>(adminProperties.rawValues);
        // Provide all default Kafka properties
        adminProperties.rawValues.putAll(sharedProperties.getRawValues());
        // Now overwrites with Consumer related
        adminProperties.rawValues.putAll(adminRawValues);
        // Register group.id if it wasn't registered
        adminProperties.rawValues.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, name);
        log.info("Building Kafka Admin: adminProperties={}", adminProperties);
        return adminProperties;
    }

}
