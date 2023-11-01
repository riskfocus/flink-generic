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

package com.ness.flink.dsl;

import java.io.Serializable;
import lombok.Value;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

/**
 * Tests Kafka-based stream builder, so running Kafka broker is a must.
 */
public class FlinkDslIT {

    @Test
    void shouldStartPipeline() {
        StreamBuilder builder = StreamBuilder.from();
        builder
            .fromKafka()
            .json("default.source", TestMessage.class)
            .noKey()
//            .key(TestMessage::getType)
            .process("test.processor", new TestProcessor());
//            .toKafka()
//            .avro("avro.sink")
        builder
            .run("testJob");
    }

    @Value
    private static class TestMessage implements Serializable {

        private static final long serialVersionUID = 1L;

        String type;
        long numericValue;

    }

    private static class TestProcessor extends ProcessFunction<TestMessage, Long> {

        @Override
        public void processElement(TestMessage value, Context ctx, Collector<Long> out) throws Exception {
            System.out.println(value);
        }

    }
}
