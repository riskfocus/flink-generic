package com.riskfocus.dsl;

import com.riskfocus.dsl.definition.KeyedProcessorDefinition;
import com.riskfocus.dsl.properties.KeyedProcessorProperties;
import com.riskfocus.dsl.test.TestEventString;
import com.riskfocus.dsl.test.TestSourceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

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
                .addSink(() -> new SinkFunction<>() {
                    @Override
                    public void invoke(TestEventString value, Context context) throws Exception {
                        VALUES.add(value);
                    }
                })
                .build()
                .run("test");

        Assertions.assertEquals(2, VALUES.size());
        Assertions.assertTrue(VALUES.containsAll(Set.of(TestEventString.fromKey("one"), TestEventString.fromKey("two"))));
    }

    private static class TestProcessFunction extends KeyedProcessFunction<String, String, TestEventString> {

        @Override
        public void processElement(String value, Context ctx, Collector<TestEventString> out) throws Exception {
            out.collect(TestEventString.fromKey(value));
        }

    }

}