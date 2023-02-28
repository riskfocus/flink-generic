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

package com.ness.flink.test.example.metrics;

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