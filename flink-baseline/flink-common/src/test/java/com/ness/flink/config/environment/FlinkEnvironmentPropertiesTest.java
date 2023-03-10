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
        Assertions.assertEquals(8789, from.getJmxReporterPort(), "Wrong default jmxReportPort");
        Assertions.assertEquals(9249, from.getPrometheusReporterPort(),
            "Wrong default prometheusReporterPort");

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