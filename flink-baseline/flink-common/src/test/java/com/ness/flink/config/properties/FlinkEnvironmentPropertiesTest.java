/*
 * Copyright 2020-2023 Ness USA, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ness.flink.config.properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import java.util.Map;

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
        Assertions.assertEquals(500, properties.getAutoWatermarkInterval(), "See overwritten values in application.yml");
        Assertions.assertEquals(10_000, properties.getMetricsFetcherUpdateInterval());
        Assertions.assertNull(properties.getCheckpointsDataUri());
    }
}