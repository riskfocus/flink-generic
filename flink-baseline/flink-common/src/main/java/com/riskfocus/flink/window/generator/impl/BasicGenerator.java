package com.riskfocus.flink.window.generator.impl;

import com.riskfocus.flink.window.WindowAware;
import com.riskfocus.flink.window.WindowContext;
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

    private static final long zero = calculateZeroTime();

    private final long windowDuration;

    protected long generateWindow(long timestamp) {
        if (timestamp < 0) {
            throw new IllegalArgumentException("timestamp cannot be negative");
        }
        long windowId = 1 + (timestamp - zero) / windowDuration;
        if (windowId < 0) {
            throw new IllegalArgumentException("Window id cannot be negative");
        }
        return windowId;
    }

    @Override
    public WindowContext generateWindowPeriod(long timestamp) {
        long windowId = generateWindow(timestamp);
        long startEpoch = zero + (windowId - 1) * windowDuration;
        long endEpoch = startEpoch + windowDuration;
        if (endEpoch < timestamp) {
            throw new IllegalArgumentException("Got wrong time calculation");
        }
        return new WindowContext(windowId, startEpoch, endEpoch);
    }

    @Override
    public long convertToTimestamp(long windowId) {
        return (windowDuration * (windowId - 1)) + zero;
    }

    private static long calculateZeroTime() {
        return 0;
    }

}