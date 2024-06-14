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

import com.ness.flink.canary.pipeline.domain.TriggerEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

@Slf4j
public class KafkaConfigsGenerator implements SourceFunction<TriggerEvent> {
    private static final long serialVersionUID = 1L;
    private final String topic;

    /** Create a bounded KafkaConfigsGenerator with the specified params */
    public KafkaConfigsGenerator(String topic) {
        this.topic = topic;
    }

    @Override
    public void run(SourceContext<TriggerEvent> ctx) {
        // Generate only one data for now
        TriggerEvent triggerEvent = new TriggerEvent(topic);

        ctx.collect(triggerEvent);
    }

    @Override
    public void cancel() { }
}
