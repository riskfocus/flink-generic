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

import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import java.time.Duration;
import java.util.Map;

import static com.ness.flink.config.properties.WatermarkType.*;


/**
 * @author Khokhlov Pavel
 */
class WatermarkPropertiesTest {

    @Test
    void shouldGetDefaultWatermark() {
        WatermarkProperties properties = WatermarkProperties.from("order.source", ParameterTool.fromMap(Map.of()));
        Assertions.assertEquals(1000, properties.getIdlenessMs());
        Assertions.assertEquals(10000, properties.getWindowSizeMs());
        Assertions.assertEquals(MONOTONOUS_TIMESTAMPS, properties.getWatermarkType());
    }

    @Test
    void shouldGetSharedWatermark() {
        WatermarkProperties properties = WatermarkProperties.from("test.sink",
                ParameterTool.fromMap(Map.of()), "/application-test.yml");
        Assertions.assertEquals(300, properties.getIdlenessMs());
        Assertions.assertEquals(5000, properties.getWindowSizeMs());
        Assertions.assertEquals(MONOTONOUS_TIMESTAMPS, properties.getWatermarkType());
    }

    @Test
    void shouldGetDefaults() {
        WatermarkProperties properties = WatermarkProperties.from(ParameterTool.fromMap(Map.of()));
        Assertions.assertEquals(1000, properties.getIdlenessMs());
        Assertions.assertEquals(10000, properties.getWindowSizeMs());
        Assertions.assertEquals(MONOTONOUS_TIMESTAMPS, properties.getWatermarkType());
    }

    @Test
    void shouldGetCustomWatermark() {
        WatermarkProperties properties = WatermarkProperties.from("custom.watermark.sink",
                ParameterTool.fromMap(Map.of()), "/application-test.yml");
        Assertions.assertEquals(400, properties.getIdlenessMs());
        Assertions.assertEquals(8000, properties.getWindowSizeMs());
        Assertions.assertEquals(BOUNDED_OUT_OF_ORDER_NESS, properties.getWatermarkType());
    }

    @Test
    void shouldGetCustomWithIdleWatermark() {
        WatermarkProperties properties = WatermarkProperties.from("customWithIdle.watermark.sink",
                ParameterTool.fromMap(Map.of()), "/application-test.yml");
        Assertions.assertEquals(0, properties.getIdlenessMs());
        Assertions.assertEquals(Duration.ZERO, properties.buildIdlenessDetectionDuration());
        Assertions.assertEquals(Duration.ZERO, properties.buildProcessingTimeTrailingDuration());
        Assertions.assertEquals(Duration.ZERO, properties.buildMaxOutOfOrderliness());
        Assertions.assertEquals(5000, properties.getWindowSizeMs());
        Assertions.assertEquals(CUSTOM_WITH_IDLE, properties.getWatermarkType());
    }

    @Test
    @SetEnvironmentVariable(key = "WATERMARK_MAX_OUT_OF_ORDERLINESS", value = "10000")
    @SetEnvironmentVariable(key = "WATERMARK_IDLENESS_DETECTION_DURATION", value = "45000")
    @SetEnvironmentVariable(key = "WATERMARK_PROCESSING_TIME_TRAILING_DURATION", value = "50")
    void shouldGetCustomWatermarkWithCustomization() {
        // WATERMARK
        WatermarkProperties properties = WatermarkProperties.from("customWithIdle.watermark.sink",
                ParameterTool.fromMap(Map.of()), "/application-test.yml");
        Assertions.assertEquals(0, properties.getIdlenessMs());
        Assertions.assertEquals(Duration.ofMillis(10000), properties.buildMaxOutOfOrderliness());
        Assertions.assertEquals(Duration.ofMillis(45000), properties.buildIdlenessDetectionDuration());
        Assertions.assertEquals(Duration.ofMillis(50), properties.buildProcessingTimeTrailingDuration());
        Assertions.assertEquals(5000, properties.getWindowSizeMs());
        Assertions.assertEquals(CUSTOM_WITH_IDLE, properties.getWatermarkType());
    }

}