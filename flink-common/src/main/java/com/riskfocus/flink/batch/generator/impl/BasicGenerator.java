package com.riskfocus.flink.batch.generator.impl;

import com.riskfocus.flink.batch.BatchAware;
import com.riskfocus.flink.batch.BatchContext;
import lombok.ToString;

import java.io.Serializable;

/**
 * @author Khokhlov Pavel
 */
@ToString
public class BasicGenerator implements Serializable, BatchAware {
    private static final long serialVersionUID = 1670857446143857575L;

    private static final long zero = calculateZeroTime();

    private final long windowDuration;

    public BasicGenerator(long windowDuration) {
        this.windowDuration = windowDuration;
    }

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
    public BatchContext generateWindowPeriod(long timestamp) {
        long windowId = generateWindow(timestamp);
        long startEpoch = zero + (windowId - 1) * windowDuration;
        long endEpoch = startEpoch + windowDuration;
        if (endEpoch < timestamp) {
            throw new IllegalArgumentException("Got wrong time calculation");
        }
        return new BatchContext(windowId, startEpoch, endEpoch);
    }

    @Override
    public long convertToTimestamp(long windowId) {
        return (windowDuration * (windowId - 1)) + zero;
    }

    private static long calculateZeroTime() {
        return 0;
    }

}