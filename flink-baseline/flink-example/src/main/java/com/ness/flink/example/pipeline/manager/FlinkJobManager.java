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

package com.ness.flink.example.pipeline.manager;


import com.ness.flink.config.operator.DefaultSource;
import com.ness.flink.example.pipeline.domain.intermediate.StreamMessage;
import com.ness.flink.example.pipeline.domain.intermediate.StreamMessage.Latency;
import com.ness.flink.example.pipeline.domain.intermediate.TestInput;
import com.ness.flink.example.pipeline.domain.intermediate.TestOutput;
import com.ness.flink.stream.FlinkDataStream;
import com.ness.flink.stream.StreamBuilder;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FlinkJobManager {

    private static final Random RANDOM = new Random();

    public static void runJob(String... args) {
        StreamBuilder streamBuilder = StreamBuilder.from(args);

        FlinkDataStream<StreamMessage<TestInput>> inputs = streamBuilder
            .stream()
            .source(new DefaultSource<TestInput>("test") {
                @Override
                public SingleOutputStreamOperator<TestInput> build(
                    StreamExecutionEnvironment streamExecutionEnvironment) {
                    return streamExecutionEnvironment.fromElements(TestInput.class, generateInputs(10000));
                }

                @Override
                public Optional<Integer> getMaxParallelism() {
                    return Optional.of(4);
                }

            })
//            .sourcePojo("option.price.source", TestInput.class, (event, recordTimestamp) -> event.getCreateTimestamp())
            .addToStream(o -> o.map(StreamMessage::from).uid("wrap"));

        inputs
            .addToStream(o -> o
                .map(new MapFunction<StreamMessage<TestInput>, StreamMessage<TestOutput>>() {
                    @Override
                    public StreamMessage<TestOutput> map(StreamMessage<TestInput> value) throws Exception {
                        long start = System.currentTimeMillis();

                        Thread.sleep(RANDOM.nextInt(20, 55)); // emulate work

                        return value.with(start,
                            new TestOutput(value.getValue().getTransactionId(), "jehfgjsdhfgjksdhg"));
                    }
                }).uid("process"))
            .addToStream(o -> o.process(new ProcessFunction<StreamMessage<TestOutput>, Void>() {

                private transient AtomicLong store;
                private transient Gauge<Long> latency;

                @Override
                public void processElement(StreamMessage<TestOutput> value, Context ctx, Collector<Void> out)
                    throws Exception {
                    List<Entry<String, Latency>> latencies = value.getLatencies().entrySet().stream().toList();
                    Latency first = latencies.get(0).getValue();
                    Latency last = latencies.get(latencies.size() - 1).getValue();
                    store.set(last.getEndTimestamp() - first.getStartTimestamp());
                    latency.getValue();

                    log.info(value.printLatencies());
                }

                @Override
                public void open(Configuration parameters) throws Exception {
                    super.open(parameters);
                    store = new AtomicLong(0L);
                    latency = getRuntimeContext().getMetricGroup().gauge("custom-latency", () -> store.longValue());
                }
            }).uid("aggregate"));

        streamBuilder.run("test");
    }

    private static TestInput[] generateInputs(int count) {
        long now = System.currentTimeMillis();

        return IntStream.range(0, count)
            .mapToObj(
                i -> new TestInput(RANDOM.nextLong(), now + ((long) i * RANDOM.nextInt(10, 50)), "uhruty"))
            .toArray(TestInput[]::new);
    }

}