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

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.USER_INFO_CONFIG;


/**
 * @author Khokhlov Pavel
 */
class KafkaProducerPropertiesTest {

    @Test
    void shouldGetDefaultValues() {
        KafkaProducerProperties properties = KafkaProducerProperties.from("defaultSource", ParameterTool.fromMap(Map.of()));
        Assertions.assertNull(properties.getParallelism());
        Assertions.assertNull(properties.getTopic());

        Assertions.assertNotNull(properties.getDeliveryGuarantee());
        Assertions.assertEquals(DeliveryGuarantee.AT_LEAST_ONCE, properties.getDeliveryGuarantee());
        Assertions.assertNull(properties.getTransactionIdPrefix());

        Assertions.assertEquals("http://localhost:8085", properties.getConfluentSchemaRegistry());
        Map<String, String> confluentRegistryConfigs = properties.getConfluentRegistryConfigs();
        Assertions.assertEquals(3, confluentRegistryConfigs.size());
        Assertions.assertEquals("URL", confluentRegistryConfigs.get(BASIC_AUTH_CREDENTIALS_SOURCE));
        Assertions.assertEquals("user:pwd", confluentRegistryConfigs.get(USER_INFO_CONFIG));

        Properties producerProperties = properties.getProducerProperties();
        Assertions.assertEquals("localhost:29092", producerProperties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));

        Assertions.assertEquals("all", producerProperties.getProperty(ProducerConfig.ACKS_CONFIG));
        Assertions.assertEquals("16384", producerProperties.getProperty(ProducerConfig.BATCH_SIZE_CONFIG));
        Assertions.assertEquals("0", producerProperties.getProperty(ProducerConfig.LINGER_MS_CONFIG));
    }

    @Test
    void shouldGetValues() {
        KafkaProducerProperties properties = buildTest();

        Assertions.assertNotNull(properties.getDeliveryGuarantee());
        Assertions.assertEquals(DeliveryGuarantee.EXACTLY_ONCE, properties.getDeliveryGuarantee());
        Assertions.assertEquals("testSink", properties.getTransactionIdPrefix());


        Assertions.assertEquals(8, properties.getParallelism());
        Assertions.assertEquals("test.sink", properties.getName());
        Assertions.assertEquals("test-out", properties.getTopic());

        Properties producerProperties = properties.getProducerProperties();

        Assertions.assertEquals("localhost:29092", producerProperties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    @SetEnvironmentVariable(key = "KAFKA_PRODUCER_BOOTSTRAP_SERVERS", value = "kafka:9090")
    void shouldOverwriteFromArgsAndEnv() {
        KafkaProducerProperties properties = buildTest();
        Properties producerProperties = properties.getProducerProperties();
        Assertions.assertEquals("kafka:9090", producerProperties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    @SetEnvironmentVariable(key = "KAFKA_PRODUCER_BOOTSTRAP_SERVERS", value = "kafka:9090")
    void shouldNotOverwriteFromArgs() {
        KafkaProducerProperties  properties = KafkaProducerProperties.from("test.sink", ParameterTool.fromMap(
                Map.of("kafka.producer.bootstrap.servers", "kafka:9095")), "/application-test.yml");
        Properties producerProperties = properties.getProducerProperties();
        Assertions.assertEquals("kafka:9095", producerProperties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), "Argument has higher priority than ENV property");
    }

    private KafkaProducerProperties buildTest() {
        return KafkaProducerProperties.from("test.sink", ParameterTool.fromMap(Map.of()), "/application-test.yml");
    }

}