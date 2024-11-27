/*
 * Copyright 2021-2024 Ness Digital Engineering
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.ness.flink.example.pipeline.domain.intermediate;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class StreamMessage<T> implements Serializable {

    LinkedHashMap<String, Latency> latencies;
    long transactionId;
    T value;

    public static StreamMessage<TestInput> from(TestInput value) {
        StreamMessage<TestInput> streamMessage = new StreamMessage<>(new LinkedHashMap<>(), value.transactionId, value);
        streamMessage.getLatencies().put("consumed", Latency.captured(value.getCreateTimestamp()));

        return streamMessage;
    }

    public StreamMessage<TestOutput> with(long start, TestOutput testOutput) {
        latencies.put("process", Latency.captured(start));

        return new StreamMessage<>(latencies, testOutput.transactionId, testOutput);
    }

    public String printLatencies() {
        return "Latencies: transactionId=" + transactionId + "\n " + latencies.entrySet().stream()
            .map(e -> e.getKey() + ":" + e.getValue().duration())
            .collect(Collectors.joining("\n"));
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Latency {

        long startTimestamp;
        long endTimestamp;

        static Latency captured(long startTimestamp) { // prevents
            return new Latency(Math.min(startTimestamp, System.currentTimeMillis()), System.currentTimeMillis());
        }

        public String duration() {
            return endTimestamp - startTimestamp + " ms";
        }
    }

}