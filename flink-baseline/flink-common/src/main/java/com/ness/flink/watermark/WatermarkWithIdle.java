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

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

import javax.validation.ValidationException;
import java.time.Duration;

/**
 * This is a {@link WatermarkWithIdle} used to emit Watermarks that lag behind the
 * element with the maximum timestamp (in event time) seen so far by a fixed amount of time,
 * {@code maxOutOfOrderness}. This can help reduce the number of elements that are ignored due to lateness when
 * computing the final result for a given window, in the case where we know that elements arrive no
 * later than {@code maxOutOfOrderness} units of time after the watermark that signals that the system
 * event-time has advanced past their (event-time) timestamp.
 *
 * <p>In addition, this extractor will detect when there have not been any records for a given
 * time (the {@code idlenessDetectionDuration}). When the stream is considered idle this assigner
 * will emit a watermark that trails behind the current processing-time by
 * {@code processingTimeTrailingDuration}.
 */
@Slf4j
public class WatermarkWithIdle<T> implements WatermarkGeneratorSupplier<T> {
    private static final long serialVersionUID = -3132268849554279554L;

    /**
     * The (fixed) interval between the maximum seen timestamp seen in the records
     * and that of the watermark to be emitted.
     */
    private final Duration maxOutOfOrderness;

    /**
     * After not extracting a timestamp for this duration the stream is considered idle. We will
     * then generate a watermark that trails by a certain amount behind processing time.
     */
    private final Duration idlenessDetectionDuration;

    /**
     * The amount by which the idle-watermark trails behind current processing time.
     */
    private final Duration processingTimeTrailingDuration;

    public WatermarkWithIdle(Duration maxOutOfOrderness,
                             Duration idlenessDetectionDuration,
                             Duration processingTimeTrailingDuration) {
        this.maxOutOfOrderness = maxOutOfOrderness;
        this.idlenessDetectionDuration = idlenessDetectionDuration;
        this.processingTimeTrailingDuration = processingTimeTrailingDuration;
    }

    public WatermarkWithIdle(Duration maxOutOfOrderness,
                             Duration idlenessDetectionDuration) {
        this(maxOutOfOrderness, idlenessDetectionDuration, Duration.ZERO);
    }

    @Override
    public WatermarkGenerator<T> createWatermarkGenerator(Context context) {
        return new WindowBasedWatermarkGenerator<>(maxOutOfOrderness.toMillis(), idlenessDetectionDuration.toMillis(),
                processingTimeTrailingDuration.toMillis());
    }

    @VisibleForTesting
    static class WindowBasedWatermarkGenerator<E> implements WatermarkGenerator<E> {
        /**
         * The (fixed) interval between the maximum seen timestamp seen in the records
         * and that of the watermark to be emitted.
         */
        private final long maxOutOfOrderness;

        /**
         * After not extracting a timestamp for this duration the stream is considered idle. We will
         * then generate a watermark that trails by a certain amount behind processing time.
         */
        private final long idlenessDetectionDuration;

        /**
         * The amount by which the idle-watermark trails behind current processing time.
         */
        private final long processingTimeTrailingDuration;

        /** The current maximum timestamp seen so far. */
        private long currentMaxTimestamp;

        /** The timestamp of the last emitted watermark. */
        private long lastEmittedWatermark = Long.MIN_VALUE;

        long lastUpdatedTimestamp = Long.MAX_VALUE;

        private WindowBasedWatermarkGenerator(long maxOutOfOrderness, long idlenessDetectionDuration,
                                             long processingTimeTrailingDuration) {
            if (maxOutOfOrderness < 0) {
                throw new ValidationException(
                        String.format("The maximum out-of-orderness cannot be negative, provided: %s",
                                maxOutOfOrderness));
            }
            if (idlenessDetectionDuration < 0) {
                throw new ValidationException(
                        String.format("The idleness detection duration cannot be negative, provided: %s",
                                idlenessDetectionDuration));

            }
            if (processingTimeTrailingDuration < 0) {
                throw new ValidationException(
                        String.format("The processing-time trailing duration cannot be negative, provided: %s",
                                processingTimeTrailingDuration));
            }
            this.maxOutOfOrderness = maxOutOfOrderness;
            this.idlenessDetectionDuration = idlenessDetectionDuration;
            this.processingTimeTrailingDuration = processingTimeTrailingDuration;

            this.currentMaxTimestamp = Long.MIN_VALUE + maxOutOfOrderness;
        }

        @Override
        public void onEvent(E event, long eventTimestamp, WatermarkOutput output) {
            if (eventTimestamp > currentMaxTimestamp) {
                currentMaxTimestamp = eventTimestamp;
            }
            lastUpdatedTimestamp = currentTimeMs();
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            long now = currentTimeMs();
            log.debug("Triggered periodicEmit: now={}", now);
            // initialize the lastUpdatedTimestamp here in case we never saw an element
            if (lastUpdatedTimestamp == Long.MAX_VALUE) {
                lastUpdatedTimestamp = now;
            }
            // this guarantees that the watermark never goes backwards.
            long potentialWM = currentMaxTimestamp - maxOutOfOrderness;

            if (potentialWM > lastEmittedWatermark) {
                // update based on timestamps if we see progress
                lastEmittedWatermark = potentialWM;
            } else if ((now - lastUpdatedTimestamp) > idlenessDetectionDuration) {
                // emit a watermark based on processing time if we don't see elements for a while
                long processingTimeWatermark = now - processingTimeTrailingDuration;
                if (processingTimeWatermark > lastEmittedWatermark) {
                    lastEmittedWatermark = processingTimeWatermark;
                }
            } else {
                log.debug("lastEmittedWatermark update skipped, because of difference between current time and lastUpdatedTimestamp: {}", now - lastUpdatedTimestamp);
            }
            log.debug("Emitting watermark with lastEmittedWatermark={}", lastEmittedWatermark);
            output.emitWatermark(new Watermark(lastEmittedWatermark));
        }

        protected long currentTimeMs() {
            return System.currentTimeMillis();
        }
    }

}
