package com.ness.flink.config.environment;


import com.ness.flink.config.properties.FlinkEnvironmentProperties;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import java.util.Map;

/**
 * @author Khokhlov Pavel
 */
class FlinkEnvironmentPropertiesTest {

    @Test
    void shouldOverwriteDefaultValues() {
        FlinkEnvironmentProperties from = FlinkEnvironmentProperties.from(ParameterTool
                .fromMap(Map.of("localDev", "true", "localPortWebUi", "1234")));

        Assertions.assertFalse(from.isEnabledObjectReuse(), "By default enabledObjectReuse should be false");
        Assertions.assertTrue(from.isLocalDev(), "By default localDev=false but in Test we enabled it");
        Assertions.assertEquals(1234, from.getLocalPortWebUi(), "Default value of localPortWebUi must be overwritten");
    }

    @Test
    @SetEnvironmentVariable(key = "ENVIRONMENT_BUFFER_TIMEOUT_MS", value = "1000")
    void shouldOverwriteBufferTimeoutMsViaEnvironmentVariable() {
        FlinkEnvironmentProperties from = FlinkEnvironmentProperties.from(ParameterTool.fromMap(Map.of()));
        Assertions.assertEquals(1000L, from.getBufferTimeoutMs());
    }

    @Test
    @SetEnvironmentVariable(key = "ENVIRONMENT_LOCAL_DEV", value = "true")
    void shouldOverwriteFromArgsAndEnv() {
        FlinkEnvironmentProperties from = FlinkEnvironmentProperties.from(ParameterTool
                .fromMap(Map.of("localDev", "false")));
        Assertions.assertFalse(from.isLocalDev(), "Argument has higher priority than ENV property");
    }

    @Test
    @SetEnvironmentVariable(key = "ENVIRONMENT_LOCAL_DEV", value = "false")
    void shouldOverwriteFromArgs() {
        FlinkEnvironmentProperties from = FlinkEnvironmentProperties.from(ParameterTool
                .fromMap(Map.of("localDev", "true")));
        Assertions.assertTrue(from.isLocalDev(), "Argument has higher priority than ENV property");
    }

    @Test
    @SetEnvironmentVariable(key = "FLINK_LOCAL_DEV", value = "true")
    void shouldOverwriteFromEnvDefaultPrefix() {
        FlinkEnvironmentProperties from = FlinkEnvironmentProperties.from(ParameterTool
                .fromMap(Map.of()));
        // Default prefix used
        Assertions.assertFalse(from.isLocalDev(), "FLINK_LOCAL_DEV shouldn't overwrite default value");
    }

    @Test
    @SetEnvironmentVariable(key = "ENVIRONMENT_LOCAL_DEV", value = "true")
    void shouldOverwriteFromEnv() {
        FlinkEnvironmentProperties from = FlinkEnvironmentProperties.from(ParameterTool.fromMap(Map.of()));
        Assertions.assertTrue(from.isLocalDev(), "ENVIRONMENT_LOCAL_DEV should overwrite default value");
    }

    @Test
    @SetEnvironmentVariable(key = "LOCAL_DEV", value = "true")
    void shouldNotOverwriteEnv() {
        FlinkEnvironmentProperties from = FlinkEnvironmentProperties.from(ParameterTool
                .fromMap(Map.of()));
        Assertions.assertFalse(from.isLocalDev(), "LOCAL_DEV shouldn't overwrite default value because Scope wasn't limited");
    }

}