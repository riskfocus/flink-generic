package com.ness.flink.dsl;

import com.ness.flink.dsl.definition.KeyedProcessorDefinition;
import com.ness.flink.dsl.properties.KeyedProcessorProperties;
import com.ness.flink.dsl.test.TestEventString;
import com.ness.flink.dsl.test.TestSourceFunction;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@Slf4j
class StreamBuilderTest {

    private static final Set<TestEventString> VALUES = new HashSet<>();

    @Test
    void shouldExecuteJob() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        ParameterTool params = ParameterTool.fromMap(Collections.emptyMap());
        KeyedProcessorDefinition<String, String, TestEventString> processor = new KeyedProcessorDefinition<>(
            KeyedProcessorProperties.from("test", params), v -> v, new TestProcessFunction());

        StreamBuilder.from(env, params)
            .newDataStream()
            .source(() -> TestSourceFunction.from("one", "two"))
            .addKeyedProcessor(processor)
            .addToStream(stream -> stream.map(v -> {
                log.info("Read: value={}", v);
                return v;
            }))
            .addSink(() -> new SinkFunction<>() {
                @Override
                public void invoke(TestEventString value, Context context) throws Exception {
                    VALUES.add(value);
                }
            })
            .build()
            .run("test");

        Assertions.assertEquals(2, VALUES.size());
        Assertions.assertTrue(
            VALUES.containsAll(Set.of(TestEventString.fromKey("one"), TestEventString.fromKey("two"))));
    }

    private static class TestProcessFunction extends KeyedProcessFunction<String, String, TestEventString> {

        @Override
        public void processElement(String value, Context ctx, Collector<TestEventString> out) throws Exception {
            out.collect(TestEventString.fromKey(value));
        }

    }

}