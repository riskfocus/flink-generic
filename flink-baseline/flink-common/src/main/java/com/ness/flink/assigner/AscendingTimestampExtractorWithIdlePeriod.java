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

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * Combination of EventTime and Processing time approach.
 * It uses event time to drive the watermark as long as data is coming in.
 * If data stops coming in for a configurable period of time ({@link AscendingTimestampExtractorWithIdlePeriod#maxIdlePeriod})
 * then it gets marked as idle and produces one more watermark
 * at the end of the window period so that the open window gets closed
 *
 * @author Bill Wicker
 * @author Khokhlov Pavel
 */
@Slf4j
@Deprecated
public abstract class AscendingTimestampExtractorWithIdlePeriod<T> implements AssignerWithPeriodicWatermarks<T> {
    private static final long serialVersionUID = 1L;

    private final long maxIdlePeriod;
    private final long delay;

    private long currentTimestamp = Long.MIN_VALUE;
    private long lastUpdateTime = currentTimestamp;

    private AscendingTimestampExtractor.MonotonyViolationHandler violationHandler = new AscendingTimestampExtractor.LoggingHandler();

    public AscendingTimestampExtractorWithIdlePeriod(long maxIdlePeriod, long delay) {
        this.maxIdlePeriod = maxIdlePeriod;
        this.delay = delay;
    }

    public AscendingTimestampExtractorWithIdlePeriod<T> withViolationHandler(@NonNull AscendingTimestampExtractor.MonotonyViolationHandler handler) {
        this.violationHandler = handler;
        return this;
    }

    /**
     * Should return extracted timestamp from event
     * @param event event
     * @return extracted timestamp
     */
    public abstract long extractTimestamp(T event);

    @Override
    public final long extractTimestamp(T element, long elementPrevTimestamp) {
        long fromEvent = extractTimestamp(element);
        if (fromEvent >= currentTimestamp) {
            currentTimestamp = fromEvent;
            // Update
            lastUpdateTime = now();
            log.debug("Extract Timestamp - currentTimestamp: {}, lastUpdateTime: {}", currentTimestamp, lastUpdateTime);
        } else {
            // Event comes from the past
            this.violationHandler.handleViolation(fromEvent, currentTimestamp);
        }
        return fromEvent;
    }

    @Override
    public final Watermark getCurrentWatermark() {
        long watermark;
        if (currentTimestamp == Long.MIN_VALUE) {
            // Watermark initialization in case there no any Events were registered
            watermark = currentTimestamp;
        } else {
            long processingTime = now();
            if ((processingTime - lastUpdateTime) > maxIdlePeriod) {
                // In this case we haven't received any Events for that specific period of time, we just use processing time in this case
                watermark = processingTime;
            } else {
                // Use timestamp extracted from Event
                watermark = currentTimestamp;
            }
            watermark = watermark - delay;
        }
        log.debug("Latest timestamp: {}, watermark: {}", currentTimestamp, watermark);
        return new Watermark(watermark);
    }

    private long now() {
        return System.currentTimeMillis();
    }

}
