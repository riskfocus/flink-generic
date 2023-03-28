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

import com.ness.flink.config.properties.KafkaConsumerProperties.Offsets;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.config.SaslConfigs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.USER_INFO_CONFIG;


/**
 * @author Khokhlov Pavel
 */
class KafkaConsumerPropertiesTest {

    @Test
    void shouldGetDefaultValues() {
        KafkaConsumerProperties properties = KafkaConsumerProperties.from("defaultSource", ParameterTool.fromMap(Map.of()));
        Assertions.assertNull(properties.getParallelism());
        Assertions.assertEquals("defaultSource", properties.getName());
        Assertions.assertEquals("http://localhost:8085", properties.getConfluentSchemaRegistry());
        Map<String, String> confluentRegistryConfigs = properties.getConfluentRegistryConfigs();
        Assertions.assertEquals(3, confluentRegistryConfigs.size());
        Assertions.assertEquals("URL", confluentRegistryConfigs.get(BASIC_AUTH_CREDENTIALS_SOURCE));
        Assertions.assertEquals("user:pwd", confluentRegistryConfigs.get(USER_INFO_CONFIG));

        Assertions.assertEquals(Offsets.COMMITTED, properties.getOffsets());
        OffsetsInitializer offsetsInitializer = properties.getOffsetsInitializer();
        Assertions.assertEquals(OffsetResetStrategy.EARLIEST, offsetsInitializer.getAutoOffsetResetStrategy());

        Assertions.assertNull(properties.getTimestamp());
        Assertions.assertNull(properties.getTopic());

        Properties consumerProperties = properties.getConsumerProperties();
        Assertions.assertEquals("localhost:29092", consumerProperties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        Assertions.assertEquals("flinkProcessor", consumerProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
        Assertions.assertEquals("1", consumerProperties.getProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG));
        Assertions.assertEquals("500", consumerProperties.getProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG));
        Assertions.assertEquals("read_committed", consumerProperties.getProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG));
        Assertions.assertEquals("true", consumerProperties.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        Assertions.assertEquals("5000", consumerProperties.getProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG));
        Assertions.assertEquals("defaultSource", consumerProperties.getProperty(KafkaSourceOptions.CLIENT_ID_PREFIX.key()));
    }

    @Test
    void shouldGetValues() {
        KafkaConsumerProperties properties = KafkaConsumerProperties.from("test.source", ParameterTool.fromMap(Map.of()), "/application-test.yml");
        Assertions.assertEquals(4, properties.getParallelism());
        Assertions.assertEquals(8, properties.getMaxParallelism());
        Assertions.assertEquals("test.source", properties.getName());
        Assertions.assertEquals("test-topic", properties.getTopic());

        Assertions.assertEquals(123, properties.getTimestamp());
        OffsetsInitializer offsetsInitializer = properties.getOffsetsInitializer();
        Assertions.assertEquals(Offsets.TIMESTAMP, properties.getOffsets());
        Assertions.assertEquals(OffsetResetStrategy.LATEST, offsetsInitializer.getAutoOffsetResetStrategy());

        Properties consumerProperties = properties.getConsumerProperties();
        Assertions.assertEquals("localhost:9092", consumerProperties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        Assertions.assertEquals("test.source", consumerProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
        Assertions.assertEquals("test.source", consumerProperties.getProperty(KafkaSourceOptions.CLIENT_ID_PREFIX.key()));
        Assertions.assertEquals("PLAIN", consumerProperties.getProperty(SaslConfigs.SASL_MECHANISM));
        Assertions.assertEquals("SASL_SSL", consumerProperties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));

        String expectedSaslJaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required username='USER1' password='PASSWD1';";
        Assertions.assertEquals(expectedSaslJaasConfig, properties.getRawValues().get(SaslConfigs.SASL_JAAS_CONFIG));
        Assertions.assertEquals(expectedSaslJaasConfig, consumerProperties.getProperty(SaslConfigs.SASL_JAAS_CONFIG));

        Assertions.assertEquals("http://localhost:8085", properties.getConfluentSchemaRegistry());
        Map<String, String> confluentRegistryConfigs = properties.getConfluentRegistryConfigs();
        Assertions.assertEquals(3, confluentRegistryConfigs.size());
        Assertions.assertEquals("USER_INFO", confluentRegistryConfigs.get(BASIC_AUTH_CREDENTIALS_SOURCE));
        Assertions.assertEquals("12121:233232", confluentRegistryConfigs.get(USER_INFO_CONFIG));

    }

}