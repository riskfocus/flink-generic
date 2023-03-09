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

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.CLIENT_NAMESPACE;

/**
 * Common Kafka properties
 * Also provides configuration for different Schemas like Confluent or AWS Glue Schema registry
 *
 * @author Khokhlov Pavel
 */
@Slf4j
@Getter
@Setter
@SuppressWarnings("PMD.AbstractClassWithoutAbstractMethod")
public abstract class KafkaProperties {
    private String name;
    private String topic;
    private Integer parallelism;

    @ToString.Exclude
    protected Map<String, String> rawValues = new LinkedHashMap<>();
    private static final String CONFLUENT_SCHEMA_REGISTRY_KEY = "schema.registry.url";

    /**
     * Provides Confluent Schema Registry URL
     * @return Confluent Schema Registry URL
     */
    public String getConfluentSchemaRegistry() {
        String schemaUrl = getConfluentSchemaRegistryProperty(CONFLUENT_SCHEMA_REGISTRY_KEY);
        log.info("Got confluent schema registry: schemaUrl={}", schemaUrl);
        return schemaUrl;
    }

    /**
     * Provides Confluent Schema related properties
     * @return Confluent Schema related properties
     */
    public Map<String, String> getConfluentRegistryConfigs() {
        return getSchemaRegistryProperties().entrySet().stream().filter(Objects::nonNull)
                .map(p -> {
                    if (p.getKey().startsWith(CLIENT_NAMESPACE)) {
                        String newKey = p.getKey().substring(CLIENT_NAMESPACE.length());
                        return Map.entry(newKey, p.getValue());
                    } else {
                        return Map.entry(p.getKey(), p.getValue());
                    }
                }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Map<String, String> getSchemaRegistryProperties() {
        return rawValues.entrySet().stream()
                .filter(e -> e.getKey().startsWith(CLIENT_NAMESPACE))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private String getConfluentSchemaRegistryProperty(String propertyName) {
        String value = getSchemaRegistryProperties().get(propertyName);
        if (value == null) {
            return "";
        }
        return value;
    }

}
