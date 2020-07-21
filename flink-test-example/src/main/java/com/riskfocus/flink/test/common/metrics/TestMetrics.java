/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.test.common.metrics;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class TestMetrics {

    private final double throughput;
    private final Latency latency;
    private final long duration;

    @Getter
    @Builder
    public static class Latency {

        private final double avg;
        private final long max;
        private final long min;
        private final long p50;
        private final long p99;

        @Override
        public String toString() {
            return "\n\t\tAVG=" + avg + "ms" +
                    "\n\t\tMAX=" + max + "ms" +
                    "\n\t\tMIN=" + min + "ms" +
                    "\n\t\tp50=" + p50 + "ms" +
                    "\n\t\tp99=" + p99 + "ms";
        }
    }

    @Override
    public String toString() {
        return "\nTest Metrics:" +
                "\n\tThroughput = " + throughput + " messages/s" +
                "\n\tLatency: " + latency +
                "\n\tDuration=" + duration + "ms";
    }
}