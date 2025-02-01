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

import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.CLIENT_NAMESPACE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.USER_INFO_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;

import com.google.common.annotations.VisibleForTesting;
import com.ness.flink.security.Credentials;
import com.ness.flink.security.SecretsProviderFactory;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * Common Kafka properties
 * Also provides configuration for different Schemas like Confluent or AWS Glue Schema registry
 *
 * @author Khokhlov Pavel
 */
@Slf4j
@Getter
@Setter
public abstract class KafkaProperties {
    private String name;
    private String topic;
    private Integer parallelism;

    @ToString.Exclude
    protected Map<String, String> rawValues = new LinkedHashMap<>();
    private static final String CONFLUENT_SCHEMA_REGISTRY_KEY = "schema.registry.url";
    private static final String SECRET_PROVIDER_KEY = "secretProvider";
    @VisibleForTesting
    static final String SECRET_NAME = ".secret.name";

    private static final CharSequence KAFKA_USERNAME_TEMPLATE = "{API_USERNAME}";
    private static final CharSequence KAFKA_PASSWORD_TEMPLATE = "{API_PASSWORD}";
    private static final CharSequence MASKED_VALUE = "***";


    /**
     * Building final Kafka properties with secrets (if they were provided)
     * @param secretProviderProperties additional properties
     * @return kafka properties
     */
    public abstract Properties buildProperties(@Nullable RawProperties<?> secretProviderProperties);

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
     * @param secretProviderProperties properties required for SecretProvider initialization
     * @return Confluent Schema related properties
     */
    public Map<String, String> getConfluentRegistryConfigs(@Nullable RawProperties<?> secretProviderProperties) {
        Map<String, String> schemaData = getSchemaRegistryProperties().entrySet().stream().filter(Objects::nonNull)
            .map(p -> {
                if (p.getKey().startsWith(CLIENT_NAMESPACE)) {
                    String newKey = p.getKey().substring(CLIENT_NAMESPACE.length());
                    return Map.entry(newKey, p.getValue());
                } else {
                    return Map.entry(p.getKey(), p.getValue());
                }
            }).collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        Credentials credentials = getCredentials("schema", secretProviderProperties);
        if (credentials != null) {
            // Replace with secrets from SM
            schemaData.put(USER_INFO_CONFIG, credentials.getUsername() + ":" + credentials.getPassword());
        }
        return schemaData;
    }

    /**
     * Retrieve secrets from SM
     * @param prefix prefix of secret, could be "schema" or "bootstrap" etc
     * @param secretProviderProperties secret provider settings
     * @return Credentials if they required
     */
    protected @Nullable Credentials getCredentials(String prefix, @Nullable RawProperties<?> secretProviderProperties) {
        String secretName = rawValues.get(prefix + SECRET_NAME);
        if (secretName != null) {
            var errorMsg = String.format("Secret name provided %s", secretName);
            // we got secret name from user
            String secretProviderStr = rawValues.get(SECRET_PROVIDER_KEY);
            if (secretProviderStr == null) {
                throw new IllegalArgumentException(errorMsg + " but secret provider is empty");
            }
            if (secretProviderProperties == null) {
                throw new IllegalArgumentException(errorMsg + " but secret provider properties are empty");
            }
            Credentials credentials = SecretsProviderFactory.retrieve(SecretProvider.valueOf(secretProviderStr), secretProviderProperties, secretName);
            if (credentials == null) {
                throw new IllegalArgumentException(errorMsg + " but credentials are empty. Please check infrastructure configuration");
            }
            return credentials;
        }
        return null;
    }

    /**
     * If necessary replaces Kafka settings with credentials which could be taken from SM
     * @param kafkaProperties kafka properties
     * @param secretProviderProperties secret provider settings
     * @return masked sasl.jaas.config configuration if replacement had a place
     */
    protected String replaceKafkaCredentials(Map<String, String> kafkaProperties,
        @Nullable RawProperties<?> secretProviderProperties) {
        Credentials credentials = getCredentials("bootstrap", secretProviderProperties);
        String masked = null;
        if (credentials != null) {
            String jaasConfig = kafkaProperties.get(SASL_JAAS_CONFIG);
            if (jaasConfig != null) {
                // Masks credentials
                masked = jaasConfig.replace(KAFKA_USERNAME_TEMPLATE, MASKED_VALUE);
                masked = masked.replace(KAFKA_PASSWORD_TEMPLATE, MASKED_VALUE);
                // Build with credentials
                jaasConfig = jaasConfig.replace(KAFKA_USERNAME_TEMPLATE, credentials.getUsername());
                jaasConfig = jaasConfig.replace(KAFKA_PASSWORD_TEMPLATE, credentials.getPassword());
                kafkaProperties.put(SASL_JAAS_CONFIG, jaasConfig);
            } else {
                throw new IllegalArgumentException(
                    "Provided secret but " + SASL_JAAS_CONFIG + " configuration is empty");
            }
        }
        return masked;
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
