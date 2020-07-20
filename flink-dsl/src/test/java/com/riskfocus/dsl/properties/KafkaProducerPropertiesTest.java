package com.riskfocus.dsl.properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static com.github.stefanbirkner.systemlambda.SystemLambda.withEnvironmentVariable;
import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic.EXACTLY_ONCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class KafkaProducerPropertiesTest {

    @Test
    void shouldReturnDefaultValues() {
        KafkaProducerProperties kafkaProducerProperties = KafkaProducerProperties.from("testSink",
                ParameterTool.fromMap(Collections.emptyMap()));

        assertNull(kafkaProducerProperties.getParallelism());
        assertEquals("testSink", kafkaProducerProperties.getTopic());
        assertEquals(EXACTLY_ONCE, kafkaProducerProperties.getSemantic());

        Properties rawProperties = kafkaProducerProperties.getProducerProperties();
        assertEquals("kafka:9092", rawProperties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("all", rawProperties.getProperty(ProducerConfig.ACKS_CONFIG));
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