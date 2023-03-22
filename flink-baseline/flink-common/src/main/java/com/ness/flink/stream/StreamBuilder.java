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

import com.amazonaws.services.schemaregistry.flink.avro.GlueSchemaRegistryAvroDeserializationSchema;
import com.ness.flink.assigner.WithMetricAssigner;
import com.ness.flink.config.channel.EventTimeExtractor;
import com.ness.flink.config.channel.KeyExtractor;
import com.ness.flink.config.channel.kafka.AvroSpecificRecordSource;
import com.ness.flink.config.channel.kafka.PojoRecordSink;
import com.ness.flink.config.channel.kafka.PojoRecordSource;
import com.ness.flink.config.channel.kafka.confluent.ConfluentAvroSpecificRecordSink;
import com.ness.flink.config.channel.kafka.msk.MskAvroSpecificRecordSink;
import com.ness.flink.config.environment.EnvironmentFactory;
import com.ness.flink.config.operator.DefaultSink;
import com.ness.flink.config.operator.DefaultSource;
import com.ness.flink.config.operator.KeyedProcessorDefinition;
import com.ness.flink.config.operator.OperatorDefinition;
import com.ness.flink.config.operator.SinkDefinition;
import com.ness.flink.config.properties.AwsProperties;
import com.ness.flink.config.properties.ChannelProperties;
import com.ness.flink.config.properties.KafkaConsumerProperties;
import com.ness.flink.config.properties.KafkaProducerProperties;
import com.ness.flink.config.properties.WatermarkProperties;
import com.ness.flink.schema.PojoDeserializationSchema;
import java.io.Serializable;
import java.time.ZoneId;
import java.util.function.Function;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Provides common Flink Stream functions
 *
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
@Slf4j
@Getter
@SuppressWarnings("PMD.ExcessiveImports")
public class StreamBuilder {

    private static final String IMPLEMENTATION_REQUIRED = "Implementation required: ";
    private static final String UNKNOWN_CHANNEL_TYPE = "Unknown provided ChannelType: ";

    private final StreamExecutionEnvironment env;
    private final ParameterTool parameterTool;

    public static StreamBuilder from(String... args) {
        return from(ParameterTool.fromArgs(args));
    }

    public static StreamBuilder from(ParameterTool parameterTool) {
        StreamExecutionEnvironment env = EnvironmentFactory.from(parameterTool);
        return from(env, parameterTool);
    }

    public static StreamBuilder from(StreamExecutionEnvironment env, ParameterTool parameterTool) {
        if (log.isInfoEnabled()) {
            log.info("Time zone is: {}", ZoneId.systemDefault());
            log.info("Job params: {}", parameterTool.toMap());
        }
        return new StreamBuilder(env, parameterTool);
    }

    @SneakyThrows
    public JobExecutionResult run(String jobName) {
        printExecutionPlan();
        return env.execute(jobName);
    }

    @SneakyThrows
    public JobClient runAsync(String jobName) {
        printExecutionPlan();
        return env.executeAsync(jobName);
    }

    private void printExecutionPlan() {
        if (log.isInfoEnabled()) {
            log.info("Execution Plan: {}", env.getExecutionPlan());
        }
    }

    /**
     * Creates a new stream instance, which allows adding operators
     */
    public StreamOperation stream() {
        return new StreamOperation();
    }

    /**
     * Building Flink Sink/Source operators.
     */
    public class StreamOperation {
        /**
         * Create Source for Flink pipeline AVRO Specific based
         *
         * @param sourceName        name of source
         * @param domainClass       data domain class
         * @param timestampAssigner Timestamp Assigner
         * @param <S>               AVRO Specific record
         * @return data stream
         */
        public <S extends SpecificRecordBase> FlinkDataStream<S> sourceAvroSpecific(String sourceName,
            Class<S> domainClass,
            SerializableTimestampAssigner<S> timestampAssigner) {
            ChannelProperties channelProperties = buildChannelProperties(sourceName);
            WatermarkProperties watermarkProperties = getWatermarkProperties(sourceName);
            ChannelProperties.ChannelType type = channelProperties.getType();
            final TimestampAssignerSupplier<S> timestampAssignerFunction = assignerSupplier(sourceName,
                timestampAssigner);
            DefaultSource<S> stream;
            switch (type) {
                case KAFKA_CONFLUENT:
                    KafkaConsumerProperties kafkaConsumerProperties = KafkaConsumerProperties
                        .from(sourceName, parameterTool);
                    stream = AvroSpecificRecordSource.<S>builder()
                        .domainClass(domainClass)
                        .name(sourceName)
                        .watermarkProperties(watermarkProperties)
                        .timestampAssignerFunction(timestampAssignerFunction)
                        .valueSchema(ConfluentRegistryAvroDeserializationSchema
                            .forSpecific(domainClass, kafkaConsumerProperties.getConfluentSchemaRegistry(),
                                kafkaConsumerProperties.getConfluentRegistryConfigs()))
                        .kafkaConsumerProperties(kafkaConsumerProperties)
                        .build();
                    break;
                case KAFKA_MSK:
                    kafkaConsumerProperties = KafkaConsumerProperties
                        .from(sourceName, parameterTool);
                    AwsProperties awsProperties = AwsProperties.from(parameterTool);
                    stream = AvroSpecificRecordSource.<S>builder()
                        .domainClass(domainClass)
                        .name(sourceName)
                        .watermarkProperties(watermarkProperties)
                        .timestampAssignerFunction(timestampAssignerFunction)
                        .valueSchema(GlueSchemaRegistryAvroDeserializationSchema.forSpecific(domainClass,
                            awsProperties.getAwsGlueSchemaConfig(kafkaConsumerProperties.getTopic())))
                        .kafkaConsumerProperties(kafkaConsumerProperties)
                        .build();
                    break;
                case AWS_KINESIS:
                    throw new UnsupportedOperationException(IMPLEMENTATION_REQUIRED + type);
                default:
                    throw new IllegalArgumentException(UNKNOWN_CHANNEL_TYPE + type);
            }
            return new FlinkDataStream<>(configureSource(stream));
        }

        /**
         * Create Source for Flink pipeline, POJO based
         *
         * @param sourceName        name of source
         * @param domainClass       data domain class
         * @param timestampAssigner Timestamp Assigner
         * @param <S>               POJO record
         * @return data stream
         */
        public <S extends Serializable> FlinkDataStream<S> sourcePojo(String sourceName, Class<S> domainClass,
            SerializableTimestampAssigner<S> timestampAssigner) {
            ChannelProperties channelProperties = buildChannelProperties(sourceName);
            WatermarkProperties watermarkProperties = getWatermarkProperties(sourceName);
            ChannelProperties.ChannelType type = channelProperties.getType();
            final TimestampAssignerSupplier<S> timestampAssignerFunction = assignerSupplier(sourceName,
                timestampAssigner);
            DefaultSource<S> stream;
            switch (type) {
                case KAFKA_CONFLUENT:
                case KAFKA_MSK:
                    KafkaConsumerProperties kafkaConsumerProperties = KafkaConsumerProperties
                        .from(sourceName, parameterTool);
                    stream = PojoRecordSource.<S>builder()
                        .domainClass(domainClass)
                        .name(sourceName)
                        .watermarkProperties(watermarkProperties)
                        .timestampAssignerFunction(timestampAssignerFunction)
                        .valueSchema(new PojoDeserializationSchema<>(domainClass))
                        .kafkaConsumerProperties(kafkaConsumerProperties)
                        .build();
                    break;
                case AWS_KINESIS:
                    throw new UnsupportedOperationException(IMPLEMENTATION_REQUIRED + type);
                default:
                    throw new IllegalArgumentException(UNKNOWN_CHANNEL_TYPE + type);
            }
            return new FlinkDataStream<>(configureSource(stream));
        }

        /**
         * Generic Source function
         *
         * @param defaultSource any implementation of Source
         * @param <S>           event
         * @return source
         */
        public <S> FlinkDataStream<S> source(DefaultSource<S> defaultSource) {
            return new FlinkDataStream<>(configureSource(defaultSource));
        }

        /**
         * Provides configuration for Source
         *
         * @param defaultSource any implementation of Source
         * @param <S>           event
         * @return source
         */
        private <S> SingleOutputStreamOperator<S> configureSource(DefaultSource<S> defaultSource) {
            SingleOutputStreamOperator<S> streamSource = defaultSource.build(env);
            String name = defaultSource.getName();
            streamSource.name(name).uid(name);
            final int parallelism = defaultSource.getParallelism().orElse(env.getParallelism());
            defaultSource.getMaxParallelism().ifPresentOrElse(maxParallelism -> {
                if (parallelism > maxParallelism) {
                    streamSource.setParallelism(maxParallelism);
                } else {
                    streamSource.setParallelism(parallelism);
                }
                streamSource.setMaxParallelism(maxParallelism);
            }, () -> streamSource.setParallelism(parallelism));
            return streamSource;
        }

        private <S> TimestampAssignerSupplier<S> assignerSupplier(String sourceName,
            @Nullable SerializableTimestampAssigner<S> timestampAssigner) {
            if (timestampAssigner != null) {
                return new WithMetricAssigner<>(sourceName, timestampAssigner);
            } else {
                return null;
            }
        }

        /**
         * Sink data from provided DataStream AVRO Specific based
         *
         * @param fromStream         source stream
         * @param sinkName           name of sink
         * @param domainClass        data domain class
         * @param keyExtractor       key extractor from domain object
         * @param eventTimeExtractor timestamp extractor from domain
         * @param <S>                AVRO Specific record
         */
        public <S extends SpecificRecordBase> void sinkAvroSpecific(SinkAware<S> fromStream,
            String sinkName,
            Class<S> domainClass,
            KeyExtractor<S> keyExtractor,
            @Nullable EventTimeExtractor<S> eventTimeExtractor) {
            ChannelProperties channelProperties = buildChannelProperties(sinkName);
            ChannelProperties.ChannelType type = channelProperties.getType();
            DefaultSink<S> defaultSink;
            switch (type) {
                case KAFKA_CONFLUENT:
                    defaultSink = ConfluentAvroSpecificRecordSink.<S>builder()
                        .domainClass(domainClass)
                        .name(sinkName)
                        .eventTimeExtractor(eventTimeExtractor)
                        .keyExtractor(keyExtractor)
                        .kafkaProducerProperties(KafkaProducerProperties.from(sinkName, parameterTool))
                        .build();
                    break;
                case KAFKA_MSK:
                    defaultSink = MskAvroSpecificRecordSink.<S>builder()
                        .domainClass(domainClass)
                        .name(sinkName)
                        .keyExtractor(keyExtractor)
                        .eventTimeExtractor(eventTimeExtractor)
                        .awsProperties(AwsProperties.from(parameterTool))
                        .kafkaProducerProperties(KafkaProducerProperties.from(sinkName, parameterTool))
                        .build();
                    break;
                case AWS_KINESIS:
                    throw new UnsupportedOperationException("Implementation required: " + type);
                default:
                    throw new IllegalArgumentException("Unknown provided ChannelType: " + type);

            }
            sink(fromStream, defaultSink);
        }

        /**
         * Sink data from provided DataStream POJO based
         *
         * @param fromStream         source stream
         * @param sinkName           name of sink
         * @param domainClass        data domain class
         * @param keyExtractor       key extractor from domain object
         * @param eventTimeExtractor timestamp extractor from domain
         * @param <S>                POJO record
         */
        public <S extends Serializable> void sinkPojo(SinkAware<S> fromStream,
            String sinkName,
            Class<S> domainClass,
            KeyExtractor<S> keyExtractor,
            @Nullable EventTimeExtractor<S> eventTimeExtractor) {

            ChannelProperties channelProperties = buildChannelProperties(sinkName);
            ChannelProperties.ChannelType type = channelProperties.getType();
            DefaultSink<S> defaultSink;
            switch (type) {
                case KAFKA_CONFLUENT:
                case KAFKA_MSK:
                    defaultSink = PojoRecordSink.<S>builder()
                        .domainClass(domainClass)
                        .name(sinkName)
                        .eventTimeExtractor(eventTimeExtractor)
                        .keyExtractor(keyExtractor)
                        .kafkaProducerProperties(KafkaProducerProperties.from(sinkName, parameterTool))
                        .build();
                    break;
                case AWS_KINESIS:
                    throw new UnsupportedOperationException("Implementation required: " + type);
                default:
                    throw new IllegalArgumentException("Unknown provided ChannelType: " + type);
            }
            sink(fromStream, defaultSink);
        }

        /**
         * Generic Sink data from provided DataStream {@link Sink} based
         *
         * @param fromStream  source stream
         * @param defaultSink new sink API
         * @param <S>         record to Sink
         */
        public <S extends Serializable> void sink(SinkAware<S> fromStream, DefaultSink<S> defaultSink) {
            String name = defaultSink.getName();
            Sink<S> sink = defaultSink.build();
            fromStream.getDataStream().sinkTo(sink).name(name).uid(name)
                .setParallelism(defaultSink.getParallelism().orElse(env.getParallelism()));
        }

        /**
         * Generic Sink data from provided DataStream {@link SinkFunction} based
         *
         * @param fromStream     source stream
         * @param sinkDefinition sink definition
         * @param <S>            record to Sink
         */
        public <S extends Serializable> void sink(SinkAware<S> fromStream, SinkDefinition<S> sinkDefinition) {
            String name = sinkDefinition.getName();
            fromStream.getDataStream().addSink(sinkDefinition.buildSink()).name(name).uid(name)
                .setParallelism(sinkDefinition.getParallelism().orElse(env.getParallelism()));
        }

        private ChannelProperties buildChannelProperties(String sourceName) {
            return ChannelProperties.from(sourceName, parameterTool);
        }

        private WatermarkProperties getWatermarkProperties(String sourceName) {
            return WatermarkProperties.from(sourceName, parameterTool);
        }
    }

    /**
     * Type of Sink
     * @param <T> event type
     */
    @FunctionalInterface
    public interface SinkAware<T> {
        DataStream<T> getDataStream();
    }

    /**
     * Auxiliary data stream wrapper, providing simple process/sink operators configuration.
     *
     * @param <T> data stream event type
     */
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public class FlinkDataStream<T> implements SinkAware<T> {
        @Getter
        protected final SingleOutputStreamOperator<T> singleOutputStreamOperator;

        @Override
        public DataStream<T> getDataStream() {
            return singleOutputStreamOperator;
        }

        /**
         * Creates a common data stream wrapper, with custom {@link KeyedProcessFunction}
         */
        public <K, U> FlinkDataStream<U> addKeyedProcessor(KeyedProcessorDefinition<K, T, U> def) {
            return new FlinkDataStream<>(configureStream(def));
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
            return new FlinkDataStream<>(steps.apply(singleOutputStreamOperator));
        }

        /**
         * Adds custom sink to current data stream wrapper, without changing event type
         * @param sinkDefinition based on old SinkFunction
         * @return same data stream wrapper, with sink added
         */
        public FlinkDataStream<T> addSink(SinkDefinition<T> sinkDefinition) {
            singleOutputStreamOperator.addSink(sinkDefinition.buildSink())
                .setParallelism(sinkDefinition.getParallelism().orElse(env.getParallelism()))
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
                .setParallelism(sinkDefinition.getParallelism().orElse(env.getParallelism()))
                .name(sinkDefinition.getName()).uid(sinkDefinition.getName());
            return this;
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
                .setParallelism(defaultSink.getParallelism().orElse(env.getParallelism()))
                .name(defaultSink.getName()).uid(defaultSink.getName());
            return this;
        }

        /**
         * Returns builder instance, allowing to add a new data stream to current job
         */
        public StreamBuilder build() {
            return StreamBuilder.this;
        }

        protected <K, U> SingleOutputStreamOperator<U> configureStream(KeyedProcessorDefinition<K, T, U> def) {
            SingleOutputStreamOperator<U> operator = singleOutputStreamOperator.keyBy(def.getKeySelector())
                .process(def.getProcessFunction())
                .setParallelism(def.getParallelism().orElse(env.getParallelism()))
                .name(def.getName())
                .uid(def.getName());
            def.getReturnTypeInformation().ifPresent(operator::returns);
            return operator;
        }
    }
}
