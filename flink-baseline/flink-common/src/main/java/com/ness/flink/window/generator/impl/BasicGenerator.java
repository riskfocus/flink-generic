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

package com.ness.flink.window.generator.impl;

import com.ness.flink.window.WindowAware;
import com.ness.flink.window.WindowContext;
import lombok.AllArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

/**
 * Wall clock based generator
 *
 * @author Khokhlov Pavel
 */
@ToString
@AllArgsConstructor
public class BasicGenerator implements Serializable, WindowAware {
    private static final long serialVersionUID = 1670857446143857575L;

    private static final long ZERO = calculateZeroTime();

    private final long windowDuration;

    protected long generateWindow(long timestamp) {
        if (timestamp < 0) {
            throw new IllegalArgumentException("timestamp cannot be negative");
        }
        long windowId = 1 + (timestamp - ZERO) / windowDuration;
        if (windowId < 0) {
            throw new IllegalArgumentException("Window id cannot be negative");
        }
        return windowId;
    }

    @Override
    public WindowContext generateWindowPeriod(long timestamp) {
        long windowId = generateWindow(timestamp);
        long startEpoch = ZERO + (windowId - 1) * windowDuration;
        long endEpoch = startEpoch + windowDuration;
        if (endEpoch < timestamp) {
            throw new IllegalArgumentException("Got wrong time calculation");
        }
        return new WindowContext(windowId, startEpoch, endEpoch);
    }

    private static long calculateZeroTime() {
        return 0;
    }

}