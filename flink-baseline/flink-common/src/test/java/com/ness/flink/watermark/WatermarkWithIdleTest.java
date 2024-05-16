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

package com.ness.flink.watermark;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

import com.ness.flink.watermark.WatermarkWithIdle.WindowBasedWatermarkGenerator;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@Slf4j
class WatermarkWithIdleTest {

    @Test
    void shouldCheckCustomWatermarkWithIdle() {
        // time when we are waiting for new event and compare previous one with wall clock time
        var idlenessDetectionDuration = 5000L;

        var watermarkWithIdle = new WatermarkWithIdle<TestPojo>(Duration.ZERO, Duration.ofMillis(idlenessDetectionDuration));
        WindowBasedWatermarkGenerator<TestPojo> watermarkGenerator = (WindowBasedWatermarkGenerator<TestPojo>)
            watermarkWithIdle.createWatermarkGenerator(() -> null);
        long now = 1715860522677L;

        watermarkGenerator = Mockito.spy(watermarkGenerator);
        var watermarkOutput = Mockito.mock(WatermarkOutput.class);

        Assertions.assertEquals(Long.MAX_VALUE, watermarkGenerator.lastUpdatedTimestamp);
        doReturn(now).when(watermarkGenerator).currentTimeMs();

        watermarkGenerator.onEvent(new TestPojo(), now, watermarkOutput);
        Assertions.assertEquals(now, watermarkGenerator.lastUpdatedTimestamp);
        watermarkGenerator.onPeriodicEmit(watermarkOutput);
        Mockito.verify(watermarkOutput, Mockito.only()).emitWatermark(new Watermark(now));

        // We didn't get any new events for amount of time
        doReturn(now + idlenessDetectionDuration).when(watermarkGenerator).currentTimeMs();
        Mockito.reset(watermarkOutput);
        watermarkGenerator.onPeriodicEmit(watermarkOutput);
        Mockito.verify(watermarkOutput, Mockito.only()).emitWatermark(new Watermark(now));
        // Now we should get updated watermark since we reached idleness detection duration
        doReturn(now + idlenessDetectionDuration + 1).when(watermarkGenerator).currentTimeMs();
        Mockito.reset(watermarkOutput);
        watermarkGenerator.onPeriodicEmit(watermarkOutput);
        // new watermark
        Mockito.verify(watermarkOutput, Mockito.only()).emitWatermark(new Watermark(now + idlenessDetectionDuration + 1));


        // New case. We got new event from stream
        Mockito.reset(watermarkOutput);
        // new event
        watermarkGenerator.onEvent(new TestPojo(), now + (idlenessDetectionDuration * 2) + 1, watermarkOutput);
        // we never emit data on event
        Mockito.verify(watermarkOutput, Mockito.never()).emitWatermark(any());
        watermarkGenerator.onPeriodicEmit(watermarkOutput);
        // now we should emit updated watermark
        Mockito.verify(watermarkOutput, Mockito.only()).emitWatermark(new Watermark(now + (idlenessDetectionDuration * 2) + 1));

    }

    static class TestPojo {
    }

}