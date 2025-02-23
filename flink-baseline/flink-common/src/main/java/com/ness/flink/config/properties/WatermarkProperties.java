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

import static com.ness.flink.config.properties.WatermarkType.NO_WATERMARK;

import com.google.common.annotations.VisibleForTesting;
import com.ness.flink.window.generator.WindowGeneratorProvider;
import java.time.Duration;
import java.util.Objects;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * @author Khokhlov Pavel
 */
@Getter
@Setter
@ToString
@Slf4j
public class WatermarkProperties {
    private static final String SHARED_PROPERTY_NAME = "watermark";

    private Long idlenessMs;
    private Long maxOutOfOrderliness;
    private long windowSizeMs;
    private Long idlenessDetectionDuration;
    private Long processingTimeTrailingDuration;
    private String group;
    private Long maxAllowedWatermarkDriftMs;
    private Long alignmentUpdateIntervalMs;

    private WatermarkType watermarkType = NO_WATERMARK;

    public Duration buildMaxAllowedWatermarkDriftMsDuration() {
        return buildDuration(maxAllowedWatermarkDriftMs);
    }

    public Duration buildAlignmentUpdateInterval() {
        return buildDuration(alignmentUpdateIntervalMs, Duration.ofMillis(1000).toMillis());
    }

    private WindowGeneratorProvider.GeneratorType windowGeneratorType = WindowGeneratorProvider.GeneratorType.BASIC;

    public static WatermarkProperties from(@NonNull ParameterTool parameterTool) {
        return from(SHARED_PROPERTY_NAME, parameterTool);
    }

    public static WatermarkProperties from(@NonNull String name, @NonNull ParameterTool parameterTool) {
        return from(name, parameterTool, OperatorPropertiesFactory.DEFAULT_CONFIG_FILE);
    }

    @VisibleForTesting
    static WatermarkProperties from(@NonNull String name, @NonNull ParameterTool parameterTool,
                                    @NonNull String ymlConfigFile) {
        WatermarkProperties watermarkProperties = OperatorPropertiesFactory
                .genericProperties(name, SHARED_PROPERTY_NAME, parameterTool,
                        WatermarkProperties.class, ymlConfigFile);
        log.info("Build parameters: watermarkProperties={}", watermarkProperties);
        return watermarkProperties;
    }

    public Duration buildMaxOutOfOrderliness() {
        return buildDuration(maxOutOfOrderliness);
    }

    public Duration buildIdlenessDetectionDuration() {
        return buildDuration(idlenessDetectionDuration);
    }

    public Duration buildProcessingTimeTrailingDuration() {
        return buildDuration(processingTimeTrailingDuration);
    }

    private Duration buildDuration(Long durationMs) {
        if (durationMs != null) {
            return Duration.ofMillis(durationMs);
        }
        return Duration.ZERO;
    }

    private Duration buildDuration(Long durationMs, long defaultValue) {
        return Duration.ofMillis(Objects.requireNonNullElse(durationMs, defaultValue));
    }
}
