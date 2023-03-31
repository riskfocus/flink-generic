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

package com.ness.flink.canary.pipeline.manager;


import com.ness.flink.canary.pipeline.domain.KafkaConfigs;
import com.ness.flink.canary.pipeline.function.HealthCheckFunction;
import com.ness.flink.canary.pipeline.sources.KafkaConfigsGenerator;
import com.ness.flink.config.operator.DefaultSource;
import com.ness.flink.config.operator.KeyedProcessorDefinition;
import com.ness.flink.config.operator.SinkDefinition;
import com.ness.flink.stream.StreamBuilder;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import java.util.Optional;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FlinkJobManager {

    public static void runJob(String... args) throws Exception {
//        ParameterTool params = ParameterTool.fromArgs(args);
//
//        StreamExecutionEnvironment env = EnvironmentFactory.from(params);
//        log.info("Starting job: params={}", params.toMap());
//
//        env.getConfig().setGlobalJobParameters(params);
//
//        env.addSource(new KafkaConfigsGenerator(params)).uid("config-source")
//            .process(new HealthCheckFunction()).uid("health-check-function")
//            .print().uid("print-stdout");
//
//        env.execute("Flink Kafka Health Check App");


//        ParameterTool params = ParameterTool.fromArgs(args);
//        log.info("Starting job: params={}", params.toMap());
//
//        // Sets up the execution environment, which is the main entry point
//        // to building Flink applications.
//        StreamExecutionEnvironment env = EnvironmentConfiguration.configureEnvironment(params);
//
//        // Register parameters to access from anywhere
//        env.getConfig().setGlobalJobParameters(params);
//
//        env.addSource(new KafkaConfigsGenerator(params))
//            .process(new HealthCheckFunction())
//            .print();
//
//        // Execute program, beginning computation.
//        env.execute("Flink Kafka Health Check App");


        StreamBuilder streamBuilder = StreamBuilder.from(args);
        ParameterTool params = ParameterTool.fromArgs(args);

//TODO Add source
        DefaultSource<KafkaConfigs> configsSource = new DefaultSource<KafkaConfigs>("configs.source") {
            @Override
            public SingleOutputStreamOperator<KafkaConfigs> build(
                StreamExecutionEnvironment streamExecutionEnvironment) {
                return streamExecutionEnvironment.addSource(new KafkaConfigsGenerator(params));
            }

            @Override
            public Optional<Integer> getMaxParallelism() {
                return Optional.empty();
            }
        };

        streamBuilder.stream().source(configsSource)
            .addToStream(stream -> stream.process(new HealthCheckFunction()).uid("health-check-function"))
            .addSink(() -> new PrintSinkFunction<>());


//        FlinkDataStream<String> stringStream =
//            streamBuilder.stream().sourcePojo(
//                "string.source",
//                String.class,
//                null);

//        streamBuilder.stream().sinkPojo(
//            stringStream,
//            "string.sink",
//            String.class,
//            (KeyExtractor<String>) event -> event.getBytes(StandardCharsets.UTF_8),
//            null);

        streamBuilder.run("flink-canary");
    }
}