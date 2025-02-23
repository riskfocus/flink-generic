/*
 * Copyright 2021-2025 Ness Digital Engineering
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

package com.ness.flink.watermark;

import com.google.common.annotations.VisibleForTesting;
import com.ness.flink.window.WindowAware;
import java.io.Serial;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

@Slf4j
@RequiredArgsConstructor

public class WaterMarkWithIdleWindowAware<T> implements WatermarkGeneratorSupplier<T> {
    @Serial
    private static final long serialVersionUID = -4118815333541536152L;

    private final WindowAware windowAware;


    @Override
    public WatermarkGenerator<T> createWatermarkGenerator(Context context) {
        return new WindowBasedWatermarkGenerator<>(windowAware);
    }

    @VisibleForTesting
    @RequiredArgsConstructor
    static class WindowBasedWatermarkGenerator<E> implements WatermarkGenerator<E> {

        private final WindowAware windowAware;
        private long lastEventTime = Long.MIN_VALUE;
        private long lastEventReceived = Long.MIN_VALUE;
        private boolean idle = true;

        @Override
        public void onEvent(E event, long eventTimestamp, WatermarkOutput output) {
          if (eventTimestamp > lastEventTime) {
              lastEventTime = eventTimestamp;
          }
          lastEventReceived = now();
          idle = false;
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            if (idle) {
                return;
            }
            long watermarkMs;
            if (now() - lastEventReceived <= windowAware.windowDurationMs()) {
                watermarkMs = lastEventTime - 1;
                output.emitWatermark(new Watermark(watermarkMs));
                log.debug("Watermark emitted: watermark={}", watermarkMs);
            } else {
                watermarkMs = windowAware.generateWindowPeriod(lastEventTime).endOfWindow();
                output.emitWatermark(new Watermark(watermarkMs));
                idle = true;
                output.markIdle();
            }
        }
    }

    static long now() {
        return System.currentTimeMillis();
    }
}