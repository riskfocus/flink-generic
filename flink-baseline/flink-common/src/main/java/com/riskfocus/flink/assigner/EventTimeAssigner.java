/*
 * Copyright 2020 Risk Focus Inc
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

package com.riskfocus.flink.assigner;

import com.riskfocus.flink.domain.TimeAware;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
@AllArgsConstructor
public class EventTimeAssigner<T extends TimeAware> implements AssignerWithPeriodicWatermarks<T> {

    private static final long serialVersionUID = -9147307311993860682L;

    private final long lagTimeThresholdMs;
    private final long delay;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        // make sure timestamps are monotonously increasing, even when the system clock re-syncs
        long timestamp = now() - delay;
        log.debug("Watermark: {}", timestamp);
        return new Watermark(timestamp);
    }

    @Override
    public long extractTimestamp(T element, long previousElementTimestamp) {
        log.debug("Element is: {}", element);
        long eventTimestamp = element.getTimestamp();
        if (eventTimestamp <= 0) {
            log.warn("Weird eventTimestamp: {}, on element: {}", eventTimestamp, element);
        }
        if (lagTimeThresholdMs > 0) {
            long lag = now() - eventTimestamp;
            if (lag > lagTimeThresholdMs) {
                log.warn("Got message which was delayed on: {} ms. Maximum lag time allowed is: {} ms", lag, lagTimeThresholdMs);
            }
        }
        return eventTimestamp;
    }

    private long now() {
        return System.currentTimeMillis();
    }
}
