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
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

@Slf4j
public class KafkaConfigsGenerator implements SourceFunction<KafkaConfigs> {
    private static final long serialVersionUID = 1L;
    private final Properties properties;
    private final String targetTopic;


    /** Create a bounded KafkaConfigsGenerator with the specified params */
    public KafkaConfigsGenerator(Properties properties, String targetTopic) {
        this.properties = properties;
        this.targetTopic = targetTopic;
    }

    @Override
    public void run(SourceContext<KafkaConfigs> ctx) {

        log.info("Target Topic: {}", targetTopic);

        // Generate only one data for now
        KafkaConfigs kafkaConfigs = KafkaConfigs.builder()
            .bootStrapServers(properties.getProperty("bootstrap.servers", "localhost:9092"))
            .topic(targetTopic)
            .requestTimeoutMs(properties.getProperty("request.timeout.ms", "1000"))
            .connectionMaxIdleMs(properties.getProperty("connections.max.idle.ms", "3000"))
            .build();

        log.info("Properties in Generator: {}", properties);

        ctx.collect(kafkaConfigs);

    }

    @Override
    public void cancel() { }
}
