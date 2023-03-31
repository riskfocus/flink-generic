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

package com.ness.flink.canary.pipeline.sources;

import com.ness.flink.canary.pipeline.domain.KafkaConfigs;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class KafkaConfigsGenerator implements SourceFunction<KafkaConfigs> {
    private static final long serialVersionUID = 1L;
//    private volatile boolean running = true;
    private final ParameterTool params;

    /** Create a bounded KafkaConfigsGenerator with the specified params */
    public KafkaConfigsGenerator(ParameterTool params) {
        this.params = params;
    }

    @Override
    public void run(SourceContext<KafkaConfigs> ctx) {

        // Generate only one data for now
        KafkaConfigs kafkaConfigs = KafkaConfigs.builder()
            .bootStrapServers(params.get("bootstrap.servers", "localhost:9092"))
            .topic(params.get("topic", "test-topic"))
            .requestTimeoutMs(params.get("request.timeout.ms", "1000"))
            .connectionMaxIdleMs(params.get("connections.max.idle.ms", "3000"))
            .build();

        ctx.collect(kafkaConfigs);

    }

    @Override
    public void cancel() { }
}
