package com.ness.flink.sink.jdbc.config;

import java.util.Map;
import com.ness.flink.sink.jdbc.properties.JdbcSinkProperties;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Khokhlov Pavel
 */
class JdbcSinkPropertiesTest {

    @Test
    void shouldGetDefaultValues() {
        var jdbcSinkProperties = JdbcSinkProperties.from("test.jdbc.sink", ParameterTool.fromMap(Map.of()));
        Assertions.assertEquals("test.jdbc.sink", jdbcSinkProperties.getName());
        Assertions.assertEquals(2, jdbcSinkProperties.getParallelism());
        Assertions.assertEquals("test-user", jdbcSinkProperties.getUsername());
        Assertions.assertEquals("12345678-a", jdbcSinkProperties.getPassword());
        Assertions.assertEquals("jdbc:mysql://localhost:3306/test?useConfigs=maxPerformance",
            jdbcSinkProperties.getUrl());
    }

}