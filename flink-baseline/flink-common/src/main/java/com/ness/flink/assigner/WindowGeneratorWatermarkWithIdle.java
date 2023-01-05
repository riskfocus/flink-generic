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

package com.ness.flink.assigner;

import com.ness.flink.window.generator.impl.BasicGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * Replacement Watermark for {@link org.apache.flink.api.common.eventtime.WatermarksWithIdleness}
 * which provided Idle functionality for flink Source function
 * Original has nasty bug and doesn't produce Watermark without having new event on Stream
 * Refer to <a href="https://ness-nde.atlassian.net/browse/FSIP-100">JIRA</a>
 *
 * @author Khokhlov Pavel
 */
@Slf4j
public class WindowGeneratorWatermarkWithIdle<T> implements WatermarkGeneratorSupplier<T> {
    private static final long serialVersionUID = 5508501490307135058L;

    private final BasicGenerator basicGenerator;
    private long id = 0;

    public WindowGeneratorWatermarkWithIdle(long windowSize) {
        basicGenerator = new BasicGenerator(windowSize);
    }

    @Override
    public WatermarkGenerator<T> createWatermarkGenerator(Context context) {
        return new WatermarkGenerator<>() {
            @Override
            public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
                id = basicGenerator.generateWindowPeriod(eventTimestamp).getId();
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput output) {
                long now = now();
                long currentId = basicGenerator.generateWindowPeriod(now).getId();
                if (id > 0 && currentId != id) {
                    log.debug("Generated Watermark: W{}", currentId);
                    id = currentId;
                    output.emitWatermark(new Watermark(now));
                    output.markActive();
                } else {
                    output.markIdle();
                }
            }

            private long now() {
                return System.currentTimeMillis();
            }
        };
    }
}
