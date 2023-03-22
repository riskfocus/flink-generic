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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import java.util.Map;
import java.util.Optional;

/**
 * @author Khokhlov Pavel
 */
class FlinkEnvironmentPropertiesTest {
    @Test
    void shouldGetDefaultValues() {
        var properties = FlinkEnvironmentProperties.from(ParameterTool.fromMap(Map.of()));

        Assertions.assertFalse(properties.isLocalDev());
        Assertions.assertFalse(properties.isEnabledRocksDb());
        Assertions.assertFalse(properties.isEnabledObjectReuse());
        Assertions.assertTrue(properties.isDisableGenericTypes());
        Assertions.assertFalse(properties.isEnabledCheckpoints());
        Assertions.assertFalse(properties.isEnabledIncrementalCheckpointing());
        Assertions.assertFalse(properties.isMetricsSystemResource());

        Assertions.assertEquals(8081, properties.getLocalPortWebUi());
        Assertions.assertEquals(Runtime.getRuntime().availableProcessors(), properties.getLocalParallelism());
        Assertions.assertEquals(9249, properties.getPrometheusReporterPort());
        Assertions.assertEquals(0, properties.getBufferTimeoutMs(), "See overwritten values in application.yml");
        Assertions.assertEquals(500, properties.getAutoWatermarkInterval(),
            "See overwritten values in application.yml");
        Assertions.assertEquals(10_000, properties.getMetricsFetcherUpdateInterval());
        Assertions.assertNull(properties.getCheckpointsDataUri());
        Assertions.assertNull(properties.getRuntimeExecutionMode());
        Assertions.assertNotNull(properties.ofRuntimeExecutionMode());
        Assertions.assertEquals(Optional.empty(), properties.ofRuntimeExecutionMode());
    }

    @Test
    @SetEnvironmentVariable(key = "ENVIRONMENT_RUNTIME_EXECUTION_MODE", value = "BATCH")
    void shouldOverwriteRuntimeExecutionModeFromEnv() {
        var properties = FlinkEnvironmentProperties.from(ParameterTool.fromMap(Map.of()));
        Assertions.assertNotNull(properties.getRuntimeExecutionMode());
        Assertions.assertEquals(RuntimeExecutionMode.BATCH, properties.getRuntimeExecutionMode());
        Assertions.assertEquals(Optional.of(RuntimeExecutionMode.BATCH), properties.ofRuntimeExecutionMode());
    }
}