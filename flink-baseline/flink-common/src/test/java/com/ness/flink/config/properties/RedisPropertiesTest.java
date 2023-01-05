package com.ness.flink.config.properties;

import io.lettuce.core.RedisURI;
import java.util.Map;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Khokhlov Pavel
 */
class RedisPropertiesTest {

    @Test
    void shouldGetDefaultValues() {

        RedisProperties properties = RedisProperties.from(ParameterTool.fromMap(Map.of()));
        Assertions.assertEquals("localhost", properties.getHost());
        Assertions.assertEquals(6379, properties.getPort());
        Assertions.assertNull(properties.getPassword());

        // Requires to have password
        properties.setPassword("1");
        RedisURI redisURI = properties.build();

        Assertions.assertEquals("localhost", redisURI.getHost());
        Assertions.assertEquals(6379, redisURI.getPort());
    }
}