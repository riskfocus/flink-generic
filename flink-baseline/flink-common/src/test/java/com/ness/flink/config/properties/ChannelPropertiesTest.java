package com.ness.flink.config.properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * @author Khokhlov Pavel
 */
class ChannelPropertiesTest {

    private static final String TEST_CONFIG = "/application-test.yml";

    @Test
    void shouldGetDefaultValues() {
        ChannelProperties properties = ChannelProperties.from("defaultSource", ParameterTool.fromMap(Map.of()));
        Assertions.assertEquals(ChannelProperties.ChannelType.KAFKA_CONFLUENT, properties.getType());
    }

    @Test
    void shouldGetDefaultValuesForOrderSource() {
        ChannelProperties properties = ChannelProperties.from("order.source", ParameterTool.fromMap(Map.of()));
        Assertions.assertEquals(ChannelProperties.ChannelType.KAFKA_CONFLUENT, properties.getType());
    }

    @Test
    void shouldGetKafkaMskType() {
        ChannelProperties properties = ChannelProperties.from("test.source", ParameterTool.fromMap(Map.of()), TEST_CONFIG);
        Assertions.assertEquals(ChannelProperties.ChannelType.KAFKA_MSK, properties.getType());
    }

    @Test
    void shouldGetKafkaConfluentType() {
        // Default Configuration is KAFKA_CONFLUENT
        ChannelProperties properties = ChannelProperties.from("confluent.sink", ParameterTool.fromMap(Map.of()), TEST_CONFIG);
        Assertions.assertEquals(ChannelProperties.ChannelType.KAFKA_CONFLUENT, properties.getType());
    }

    @Test
    void shouldGetKafkaAwsKinesisType() {
        ChannelProperties properties = ChannelProperties.from("test.sink", ParameterTool.fromMap(Map.of()), TEST_CONFIG);
        Assertions.assertEquals(ChannelProperties.ChannelType.AWS_KINESIS, properties.getType());
    }

}