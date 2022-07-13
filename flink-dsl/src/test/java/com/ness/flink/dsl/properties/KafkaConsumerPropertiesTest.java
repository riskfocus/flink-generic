package com.ness.flink.dsl.properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static com.github.stefanbirkner.systemlambda.SystemLambda.withEnvironmentVariable;
import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaConsumerPropertiesTest {

    @Test
    void shouldReturnDefaultValues() {
        KafkaConsumerProperties kafkaConsumerProperties = KafkaConsumerProperties.from("testSource",
                ParameterTool.fromMap(Collections.emptyMap()));

        assertEquals(1, kafkaConsumerProperties.getParallelism());
        assertEquals("testSource", kafkaConsumerProperties.getTopic());

        Properties rawProperties = kafkaConsumerProperties.getConsumerProperties();
        assertEquals("localhost:9092", rawProperties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("testSource", rawProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals("100", rawProperties.getProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
    }

    @Test
    void shouldOverrideFromArgsAndEnv() throws Exception {
        withEnvironmentVariable("BOOTSTRAP_SERVERS", "kafka:9090")
                .execute(() -> {
                    KafkaConsumerProperties properties = KafkaConsumerProperties.from("testSource",
                            ParameterTool.fromMap(Map.of("testSource.parallelism", "4", "topic", "overridden", "group.id", "overridden")));

                    assertEquals(4, properties.getParallelism());
                    assertEquals("overridden", properties.getTopic());

                    Properties consumerProperties = properties.getConsumerProperties();
                    assertEquals("kafka:9090", consumerProperties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
                    assertEquals("overridden", consumerProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
                });
    }

}