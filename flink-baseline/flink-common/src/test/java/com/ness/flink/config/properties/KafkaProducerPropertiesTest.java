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

import com.ness.flink.security.Credentials;
import com.ness.flink.security.SecretsProviderFactory;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.USER_INFO_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.mockito.ArgumentMatchers.eq;


/**
 * @author Khokhlov Pavel
 */
class KafkaProducerPropertiesTest {

    @Test
    void shouldGetDefaultValues() {

        var schemaCredentials = new Credentials("userFromSecret", "passwordFromSecret");
        var kafkaCredentials = new Credentials("kafkaUsernameFromSecret", "kafkaPasswordFromSecret");

        try (MockedStatic<SecretsProviderFactory> utilities = Mockito.mockStatic(SecretsProviderFactory.class)) {

            utilities.when(() -> SecretsProviderFactory
                .retrieve(Mockito.any(), Mockito.any(), eq("schemaRegistrySecret"))).thenReturn(schemaCredentials);

            utilities.when(() -> SecretsProviderFactory
                .retrieve(Mockito.any(), Mockito.any(), eq("kafkaSecret"))).thenReturn(kafkaCredentials);

            KafkaProducerProperties properties = KafkaProducerProperties.from("defaultSource", ParameterTool.fromMap(Map.of()));
            AwsProperties awsProperties = AwsProperties.from(ParameterTool.fromMap(Map.of()));
            Assertions.assertNull(properties.getParallelism());
            Assertions.assertNull(properties.getTopic());

            Assertions.assertNotNull(properties.getDeliveryGuarantee());
            Assertions.assertEquals(DeliveryGuarantee.AT_LEAST_ONCE, properties.getDeliveryGuarantee());
            Assertions.assertNull(properties.getTransactionIdPrefix());

            Assertions.assertEquals("http://localhost:8085", properties.getConfluentSchemaRegistry());
            Map<String, String> confluentRegistryConfigs = properties.getConfluentRegistryConfigs(awsProperties);
            Assertions.assertEquals(3, confluentRegistryConfigs.size());
            Assertions.assertEquals("URL", confluentRegistryConfigs.get(BASIC_AUTH_CREDENTIALS_SOURCE));
            Assertions.assertEquals("userFromSecret:passwordFromSecret", confluentRegistryConfigs.get(USER_INFO_CONFIG));

            Properties producerProperties = properties.getProducerProperties(awsProperties);
            Assertions.assertEquals("localhost:29092", producerProperties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
            Assertions.assertEquals("org.apache.kafka.common.security.plain.PlainLoginModule   required username='kafkaUsernameFromSecret' password='kafkaPasswordFromSecret';", producerProperties.getProperty(SASL_JAAS_CONFIG));

            Assertions.assertEquals("all", producerProperties.getProperty(ProducerConfig.ACKS_CONFIG));
            Assertions.assertEquals("16384", producerProperties.getProperty(ProducerConfig.BATCH_SIZE_CONFIG));
            Assertions.assertEquals("0", producerProperties.getProperty(ProducerConfig.LINGER_MS_CONFIG));
        }
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

        Properties producerProperties = properties.getProducerProperties(null);

        Assertions.assertEquals("localhost:29092", producerProperties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    @SetEnvironmentVariable(key = "KAFKA_PRODUCER_BOOTSTRAP_SERVERS", value = "kafka:9090")
    void shouldOverwriteFromArgsAndEnv() {
        KafkaProducerProperties properties = buildTest();
        Properties producerProperties = properties.getProducerProperties(null);
        Assertions.assertEquals("kafka:9090", producerProperties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    @SetEnvironmentVariable(key = "KAFKA_PRODUCER_BOOTSTRAP_SERVERS", value = "kafka:9090")
    void shouldNotOverwriteFromArgs() {
        KafkaProducerProperties  properties = KafkaProducerProperties.from("test.sink", ParameterTool.fromMap(
                Map.of("kafka.producer.bootstrap.servers", "kafka:9095")), "/application-test.yml");
        Properties producerProperties = properties.getProducerProperties(null);
        Assertions.assertEquals("kafka:9095", producerProperties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), "Argument has higher priority than ENV property");
    }

    private KafkaProducerProperties buildTest() {
        return KafkaProducerProperties.from("test.sink", ParameterTool.fromMap(Map.of()), "/application-test.yml");
    }

    @Test
    void testGettingCredentials() {
        KafkaProducerProperties properties = KafkaProducerProperties.from("defaultSource", ParameterTool.fromMap(Map.of()));
        AwsProperties awsProperties = AwsProperties.from(ParameterTool.fromMap(Map.of()));
        var credentials = properties.getCredentials("unknown", awsProperties);
        Assertions.assertNull(credentials, "Unregistered prefix provided, we can't get credentials");

        Assertions.assertThrows(IllegalArgumentException.class, () ->
            properties.getCredentials("bootstrap", null),
            "Secret name provided bootstrap but secret provider is empty");
    }

}