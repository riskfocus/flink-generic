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

package com.ness.flink.config.operator;


import com.ness.flink.assigner.WindowGeneratorWatermarkWithIdle;
import com.ness.flink.config.properties.WatermarkProperties;
import com.ness.flink.config.properties.WatermarkType;
import lombok.experimental.SuperBuilder;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import javax.annotation.Nullable;
import java.time.Duration;

/**
 * Any Source which support Flink Source Watermarks
 * @see WatermarkStrategy
 * @author Khokhlov Pavel
 */
@SuperBuilder
public abstract class WatermarkAwareSource<S> extends DefaultSource<S> {
    private final WatermarkProperties watermarkProperties;

    protected WatermarkStrategy<S> buildWatermarkStrategy(@Nullable TimestampAssignerSupplier<S> timestampAssignerFunction) {
        WatermarkType watermarkType = watermarkProperties.getWatermarkType();
        WatermarkStrategy<S> watermarkStrategy;
        switch (watermarkType) {
            case NO_WATERMARK:
                watermarkStrategy = WatermarkStrategy.noWatermarks();
                break;
            case MONOTONOUS_TIMESTAMPS:
                watermarkStrategy = WatermarkStrategy.forMonotonousTimestamps();
                break;
            case BOUNDED_OUT_OF_ORDER_NESS:
                watermarkStrategy = WatermarkStrategy.forBoundedOutOfOrderness(watermarkProperties.buildMaxOutOfOrderliness());
                break;
            case CUSTOM_WITH_IDLE:
                watermarkStrategy = WatermarkStrategy
                    .forGenerator(new WindowGeneratorWatermarkWithIdle<>(watermarkProperties.getWindowSizeMs()));
                break;
            default:
                throw new IllegalArgumentException("Unsupported WatermarkType: " + watermarkType);
        }
        Long watermarkIdlenessMs = watermarkProperties.getIdlenessMs();
        if (watermarkIdlenessMs != null && watermarkIdlenessMs > 0) {
            watermarkStrategy = watermarkStrategy.withIdleness(Duration.ofMillis(watermarkIdlenessMs));
        }
        if (timestampAssignerFunction != null) {
            watermarkStrategy = watermarkStrategy.withTimestampAssigner(timestampAssignerFunction);
        }
        return watermarkStrategy;
    }

}
