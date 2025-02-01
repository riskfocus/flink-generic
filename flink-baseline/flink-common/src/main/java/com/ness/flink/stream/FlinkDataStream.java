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

import com.ness.flink.config.operator.*;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import java.util.function.Function;

/**
 * Auxiliary data stream wrapper, providing simple process/sink operators configuration.
 *
 * @param <T> data stream event type
 */
@AllArgsConstructor(access = AccessLevel.PACKAGE)
@PublicEvolving
public class FlinkDataStream<T> implements DataStreamProvider<T> {

    private final StreamBuilder streamBuilder;
    @Getter
    private final SingleOutputStreamOperator<T> singleOutputStreamOperator;

    @Override
    public DataStream<T> getDataStream() {
        return singleOutputStreamOperator;
    }

    /**
     * Provides broadcastState descriptors to pipeline
     *
     * @param broadcastStateDescriptors broadcastState descriptors
     * @return BroadcastStream data
     */
    public BroadcastStream<T> broadcast(final MapStateDescriptor<?, ?>... broadcastStateDescriptors) {
        return getDataStream().broadcast(broadcastStateDescriptors);
    }

    /**
     * Creates a common data stream wrapper, with custom {@link KeyedProcessFunction}
     */
    public <K, U> FlinkDataStream<U> addKeyedProcessor(KeyedProcessorDefinition<K, T, U> def) {
        return new FlinkDataStream<>(streamBuilder, configureStream(def));
    }

    /**
     * Creates a data stream wrapper, with custom {@link KeyedBroadcastProcessorDefinition}
     */
    public <K, A, U> FlinkDataStream<U> addKeyedBroadcastProcessor(KeyedBroadcastProcessorDefinition<K, T, A, U> def,
        FlinkDataStream<A> broadcastDataStream) {
        return new FlinkDataStream<>(streamBuilder, configureStream(def, broadcastDataStream));
    }

    /**
     * Allows to modify Flink data stream directly, by calling usual Flink DataStream API methods. Example:
     * <pre>{@code
     * StreamBuilder.from(env, params)
     *     .stream()
     *     .source(sourceFunction)
     *     .addToStream(stream -> stream
     *           .keyBy(selector)
     *           .map(mapper)
     *           .addSink(sinkFunction))
     *      .build()
     *      .run(job);
     * }</pre>
     */
    public <U> FlinkDataStream<U> addToStream(Function<SingleOutputStreamOperator<T>,
        SingleOutputStreamOperator<U>> steps) {
        return new FlinkDataStream<>(streamBuilder, steps.apply(singleOutputStreamOperator));
    }

    /**
     * Adds custom sink to current data stream wrapper, without changing event type
     * @param sinkDefinition based on old SinkFunction
     * @return same data stream wrapper, with sink added
     */
    public FlinkDataStream<T> addSink(SinkDefinition<T> sinkDefinition) {
        singleOutputStreamOperator.addSink(sinkDefinition.buildSink())
            .setParallelism(sinkDefinition.getParallelism().orElse(getParallelism()))
            .name(sinkDefinition.getName()).uid(sinkDefinition.getName());
        return this;
    }

    /**
     * Adds discarding sink {@link org.apache.flink.streaming.api.functions.sink.DiscardingSink}
     *
     * @param sinkDefinition operator Definition
     * @return same data stream wrapper, with sink added
     */
    public FlinkDataStream<T> addDiscardingSink(OperatorDefinition sinkDefinition) {
        singleOutputStreamOperator.addSink(new DiscardingSink<>())
            .setParallelism(sinkDefinition.getParallelism().orElse(getParallelism()))
            .name(sinkDefinition.getName()).uid(sinkDefinition.getName());
        return this;
    }

    private int getParallelism() {
        return streamBuilder.getEnv().getParallelism();
    }

    /**
     * Adds custom sink to current data stream wrapper, without changing event type
     *
     * @param defaultSink based on new Flink Sink API
     * @return same data stream wrapper, with sink added
     */
    public FlinkDataStream<T> addSink(DefaultSink<T> defaultSink) {
        singleOutputStreamOperator
            .sinkTo(defaultSink.build())
            .setParallelism(defaultSink.getParallelism().orElse(getParallelism()))
            .name(defaultSink.getName()).uid(defaultSink.getName());
        return this;
    }

    /**
     * Returns builder instance, allowing to add a new data stream to current job
     */
    public StreamBuilder build() {
        return streamBuilder;
    }

    protected <K, U> SingleOutputStreamOperator<U> configureStream(KeyedProcessorDefinition<K, T, U> def) {
        SingleOutputStreamOperator<U> operator = singleOutputStreamOperator.keyBy(def.getKeySelector())
            .process(def.getProcessFunction())
            .setParallelism(def.getParallelism().orElse(getParallelism()))
            .name(def.getName())
            .uid(def.getName());
        def.getReturnTypeInformation().ifPresent(operator::returns);
        return operator;
    }

    protected <K, A, U> SingleOutputStreamOperator<U> configureStream(KeyedBroadcastProcessorDefinition<K, T, A, U> def,
        FlinkDataStream<A> broadcastStream) {
        SingleOutputStreamOperator<U> operator = singleOutputStreamOperator
            .keyBy(def.getKeySelector())
            .connect(def.broadcast(broadcastStream))
            .process(def.getProcessFunction())
            .setParallelism(def.getParallelism().orElse(getParallelism()))
            .name(def.getName())
            .uid(def.getName());
        def.getReturnTypeInformation().ifPresent(operator::returns);
        return operator;
    }
}
