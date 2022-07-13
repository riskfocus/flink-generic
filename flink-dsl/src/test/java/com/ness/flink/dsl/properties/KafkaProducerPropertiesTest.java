package com.ness.flink.dsl.properties;

import static com.github.stefanbirkner.systemlambda.SystemLambda.withEnvironmentVariable;
import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic.EXACTLY_ONCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

class KafkaProducerPropertiesTest {

    @Test
    void shouldReturnDefaultValues() {
        KafkaProducerProperties kafkaProducerProperties = KafkaProducerProperties.from("testSink",
            ParameterTool.fromMap(Collections.emptyMap()));

        assertNull(kafkaProducerProperties.getParallelism());
        assertEquals("testSink", kafkaProducerProperties.getTopic());
        assertTrue(kafkaProducerProperties.getSemantic().isPresent());
        assertEquals(EXACTLY_ONCE, kafkaProducerProperties.getSemantic().get());

        Properties rawProperties = kafkaProducerProperties.getProducerProperties();
        assertEquals("kafka:9092", rawProperties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("all", rawProperties.getProperty(ProducerConfig.ACKS_CONFIG));
    }

    @Test
    void shouldSupportPrefixedName() {
        KafkaProducerProperties kafkaProducerProperties = KafkaProducerProperties.from("AWSConfiguration.sink",
            ParameterTool.fromMap(Collections.emptyMap()));

        assertNull(kafkaProducerProperties.getParallelism());
        assertEquals("aws", kafkaProducerProperties.getTopic());
        assertTrue(kafkaProducerProperties.getSemantic().isEmpty());
    }

    @Test
    void shouldOverrideFromArgsAndEnv() throws Exception {
        withEnvironmentVariable("BOOTSTRAP_SERVERS", "kafka:9090")
            .execute(() -> {
                KafkaProducerProperties properties = KafkaProducerProperties.from("testSink",
                    ParameterTool.fromMap(Map.of("testSink.parallelism", "4", "topic", "overridden")));

                assertEquals(4, properties.getParallelism());
                assertEquals("overridden", properties.getTopic());

                Properties producerProperties = properties.getProducerProperties();
                assertEquals("kafka:9090", producerProperties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
            });
    }

}