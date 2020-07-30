/*
 * Copyright 2020 Risk Focus Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.riskfocus.dsl;

import com.riskfocus.dsl.definition.KeyedProcessorDefinition;
import com.riskfocus.dsl.definition.SinkDefinition;
import com.riskfocus.dsl.definition.SourceDefinition;
import com.riskfocus.dsl.definition.kafka.CommonKafkaSource;
import com.riskfocus.dsl.definition.kafka.KeyAwareJsonKafkaSink;
import com.riskfocus.dsl.properties.KafkaConsumerProperties;
import com.riskfocus.dsl.properties.KafkaProducerProperties;
import com.riskfocus.dsl.properties.KeyedProcessorProperties;
import com.riskfocus.flink.config.EnvironmentFactory;
import com.riskfocus.flink.domain.Event;
import com.riskfocus.flink.domain.FlinkKeyedAware;
import com.riskfocus.flink.domain.KafkaKeyedAware;
import com.riskfocus.flink.schema.EventDeserializationSchema;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

import java.util.function.Function;

/**
 * Main class, providing single configuration point for Flink {@link StreamExecutionEnvironment}.
 * Allows to add different operators to stream, using {@link com.riskfocus.dsl.definition.SimpleDefinition}
 * implementors, and provides specific stream types, restricted to event type, to ease configuration.
 * <p>
 * Usage example is:
 * <pre> {@code
 * StreamBuilder.from(env, params)
 *      .newDataStream()
 *      .kafkaEventSource(sourceName, TestEvent.class)
 *      .addFlinkKeyedAwareProcessor(procName, new ProcessorImpl())
 *      .addJsonKafkaSink(sinkName)
 *      .build()
 *      .runAsync(jobName);
 * } </pre>
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class StreamBuilder {

    private final StreamExecutionEnvironment env;
    private final ParameterTool params;

    /**
     * Creates builder instance from program args
     */
    public static StreamBuilder from(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = EnvironmentFactory.from(params);

        return from(env, params);
    }

    /**
     * Creates builder instance from preconfigured environment and params
     */
    public static StreamBuilder from(StreamExecutionEnvironment env, ParameterTool params) {
        return new StreamBuilder(env, params);
    }

    /**
     * Creates a new stream instance, which allows adding operators
     */
    public SourcedDataStream newDataStream() {
        return new SourcedDataStream();
    }

    /**
     * Runs all data streams created in this job
     */
    @SneakyThrows
    public void run(String jobName) {
        env.execute(jobName);
    }

    /**
     * Runs all data streams created in this job asynchronously
     */
    @SneakyThrows
    public JobClient runAsync(String jobName) {
        return env.executeAsync(jobName);
    }

    /**
     * Auxiliary class, providing only source functionality, and converting
     * data stream wrapper to {@link FlinkDataStream} instance, for next operators.
     */
    public class SourcedDataStream {

        /**
         * Creates {@link FlinkDataStream} instance with any custom source
         *
         * @param def source definition, providing config for
         *            {@link org.apache.flink.streaming.api.functions.source.SourceFunction}
         * @param <T> type of events, coming from source
         * @return non-specific data stream wrapper
         */
        public <T> FlinkDataStream<T> source(SourceDefinition<T> def) {
            return new FlinkDataStream<>(configureEnv(def));
        }

        /**
         * Creates {@link FlinkKeyedAwareDataStream} instance, where event type is bound to {@link Event},
         * providing deserialization, and to {@link FlinkKeyedAware},
         * providing {@link org.apache.flink.api.java.functions.KeySelector} implementation
         *
         * @param <K> type of event key, for {@link org.apache.flink.api.java.functions.KeySelector}
         * @param <T> type of event
         * @return keyed-aware data stream wrapper
         */
        public <K, T extends Event & FlinkKeyedAware<K>>
        FlinkKeyedAwareDataStream<K, T> kafkaEventSource(String sourceName, Class<T> valueType) {
            KafkaConsumerProperties consumerProperties = KafkaConsumerProperties.from(sourceName, params);
            CommonKafkaSource<T> source = new CommonKafkaSource<>(consumerProperties, new EventDeserializationSchema<>(valueType));

            return new FlinkKeyedAwareDataStream<>(configureEnv(source));
        }

        private <T> SingleOutputStreamOperator<T> configureEnv(SourceDefinition<T> def) {
            return env.addSource(def.buildSource())
                    .setParallelism(def.getParallelism().orElse(env.getParallelism()))
                    .name(def.getName())
                    .uid(def.getName());
        }

    }

    /**
     * Auxiliary data stream wrapper, providing process/sink operators configuration.
     *
     * @param <T> data stream event type
     */
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public class FlinkDataStream<T> {

        protected final SingleOutputStreamOperator<T> streamFromSource;

        /**
         * Creates a common data stream wrapper, with custom {@link KeyedProcessFunction}
         */
        public <K, U> FlinkDataStream<U> addKeyedProcessor(KeyedProcessorDefinition<K, T, U> def) {
            return new FlinkDataStream<>(configureStream(def));
        }

        /**
         * Adds custom sink to current data stream wrapper, without changing event type
         *
         * @return same data stream wrapper, with sink added
         */
        public FlinkDataStream<T> addSink(SinkDefinition<T> def) {
            streamFromSource.addSink(def.buildSink())
                    .setParallelism(def.getParallelism().orElse(env.getParallelism()))
                    .name(def.getName()).uid(def.getName());

            return this;
        }

        public <U> FlinkDataStream<U> addToStream(Function<SingleOutputStreamOperator<T>, SingleOutputStreamOperator<U>> steps) {
            return new FlinkDataStream<>(steps.apply(streamFromSource));
        }

        /**
         * Returns builder instance, allowing to add a new data stream to current job
         */
        public StreamBuilder build() {
            return StreamBuilder.this;
        }

        protected <K, U> SingleOutputStreamOperator<U> configureStream(KeyedProcessorDefinition<K, T, U> def) {
            return streamFromSource.keyBy(def.getKeySelector())
                    .process(def.getProcessFunction())
                    .setParallelism(def.getParallelism().orElse(env.getParallelism()))
                    .name(def.getName())
                    .uid(def.getName());
        }

    }

    /**
     * Auxiliary data stream wrapper, with event type bound to {@link FlinkKeyedAware}, easing processor configuration.
     */
    @SuppressWarnings("Convert2MethodRef") // Flink couldn't infer type from lambda reference
    public class FlinkKeyedAwareDataStream<K, T extends FlinkKeyedAware<K>> extends FlinkDataStream<T> {

        private FlinkKeyedAwareDataStream(SingleOutputStreamOperator<T> streamFromSource) {
            super(streamFromSource);
        }

        /**
         * Created a new wrapper instance, adding {@link KeyedProcessFunction} to data stream. Event type is bound to
         * {@link FlinkKeyedAware}, easing processor configuration
         *
         * @param name     process function name, to resolve its properties
         * @param function event processing function
         * @param <U>      result event type
         */
        public <U extends FlinkKeyedAware<K>>
        FlinkKeyedAwareDataStream<K, U> addFlinkKeyedAwareProcessor(String name, KeyedProcessFunction<K, T, U> function) {
            return new FlinkKeyedAwareDataStream<>(configureStream(name, function));
        }

        /**
         * Created a new wrapper instance, adding {@link KeyedProcessFunction} to data stream. Event type is bound to
         * {@link KafkaKeyedAware} type, easing next sink configuration
         *
         * @param name     process function name, to resolve its properties
         * @param function event processing function
         * @param <U>      result event type
         */
        public <U extends FlinkKeyedAware<K> & KafkaKeyedAware>
        KafkaKeyedAwareDataStream<U> addKafkaKeyedAwareProcessor(String name, KeyedProcessFunction<K, T, U> function) {
            return new KafkaKeyedAwareDataStream<>(configureStream(name, function));
        }

        private <U> SingleOutputStreamOperator<U> configureStream(String name, KeyedProcessFunction<K, T, U> function) {
            KeyedProcessorProperties properties = KeyedProcessorProperties.from(name, params);
            KeyedProcessorDefinition<K, T, U> def = new KeyedProcessorDefinition<>(properties, value -> value.flinkKey(), function);

            return configureStream(def);
        }

    }

    /**
     * Auxiliary data stream wrapper, with event type bound to {@link KafkaKeyedAware}, easing sink configuration.
     */
    public class KafkaKeyedAwareDataStream<T extends KafkaKeyedAware> extends FlinkDataStream<T> {

        private KafkaKeyedAwareDataStream(SingleOutputStreamOperator<T> streamFromSource) {
            super(streamFromSource);
        }

        /**
         * Adds kafka-specific sink  to data stream
         *
         * @see KeyAwareJsonKafkaSink
         */
        public FlinkDataStream<T> addJsonKafkaSink(String sinkName) {
            KafkaProducerProperties producerProperties = KafkaProducerProperties.from(sinkName, params);
            KeyAwareJsonKafkaSink<T> def = new KeyAwareJsonKafkaSink<>(producerProperties);

            return addSink(def);
        }

    }

}
