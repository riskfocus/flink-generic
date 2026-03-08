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

package com.ness.flink.stream;

import com.ness.flink.config.operator.DefaultSource;
import com.ness.flink.config.operator.KeyedProcessorDefinition;
import com.ness.flink.config.properties.OperatorProperties;
import com.ness.flink.stream.test.TestEventString;
import java.io.Serial;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.util.testing.CollectingSink;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ParameterTool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
class StreamBuilderTest {

    @Test
    void shouldExecuteJob() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        ParameterTool params = ParameterTool.fromMap(Collections.emptyMap());
        DefaultSource<String> testSource = new DefaultSource<>("test.source") {
            @Override
            public SingleOutputStreamOperator<String> build(StreamExecutionEnvironment streamExecutionEnvironment) {
                return streamExecutionEnvironment.fromData("one", "two");
            }

            @Override
            public Optional<Integer> getMaxParallelism() {
                return Optional.empty();
            }
        };
        CollectingSink<TestEventString> sink = new CollectingSink<>();
        StreamBuilder.from(env, params)
            .stream()
            .source(testSource)
            .addKeyedProcessor(new KeyedProcessorDefinition<>(
                OperatorProperties.from("test.processor", params), v -> v, new TestProcessFunction()))
            .addToStream(stream -> stream.map(v -> {
                log.info("Read: value={}", v);
                return v;
            }))
            .addSink(() -> sink).build().run("test.sink");
        List<TestEventString> output = sink.getRemainingOutput();
        Assertions.assertEquals(2, output.size());
        Assertions.assertTrue(
            output.containsAll(Set.of(TestEventString.fromKey("one"), TestEventString.fromKey("two"))));
    }


    private static class TestProcessFunction extends KeyedProcessFunction<String, String, TestEventString> {
        @Serial
        private static final long serialVersionUID = -2235368612899508484L;

        @Override
        public void processElement(String value, Context ctx, Collector<TestEventString> out) {
            out.collect(TestEventString.fromKey(value));
        }
    }
}