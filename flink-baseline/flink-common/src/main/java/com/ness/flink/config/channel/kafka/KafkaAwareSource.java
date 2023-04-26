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

package com.ness.flink.config.channel.kafka;

import com.ness.flink.config.operator.WatermarkAwareSource;
import com.ness.flink.config.properties.AwsProperties;
import com.ness.flink.config.properties.KafkaConsumerProperties;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Optional;


/**
 * Could be any Kafka Source function
 *
 * @author Khokhlov Pavel
 */
@SuperBuilder
abstract class KafkaAwareSource<S> extends WatermarkAwareSource<S> {
    protected final KafkaConsumerProperties kafkaConsumerProperties;
    protected final AwsProperties awsProperties;
    @Override
    public Optional<String> getTopic() { return Optional.ofNullable(kafkaConsumerProperties.getTopic()); }

    @Override
    public Optional<Integer> getParallelism() {
        return Optional.ofNullable(kafkaConsumerProperties.getParallelism());
    }

    @Override
    public Optional<Integer> getMaxParallelism() {
        return Optional.ofNullable(kafkaConsumerProperties.getMaxParallelism());
    }

    protected abstract DeserializationSchema<S> getDeserializationSchema();

    protected abstract TypeInformation<S> getTypeInformation();

    protected abstract TimestampAssignerSupplier<S> getTimestampAssignerFunction();

    @Override
    public final SingleOutputStreamOperator<S> build(StreamExecutionEnvironment streamExecutionEnvironment) {
        KafkaSource<S> source = KafkaSource.<S>builder()
                .setProperties(kafkaConsumerProperties.getConsumerProperties(awsProperties))
                .setTopics(kafkaConsumerProperties.getTopic())
                .setStartingOffsets(kafkaConsumerProperties.getOffsetsInitializer())
                .setDeserializer(WithMetricsKafkaDeserialization.build(getName(), getDeserializationSchema(),
                        kafkaConsumerProperties.isSkipBrokenMessages()))
                .build();
        WatermarkStrategy<S> timestampsAndWatermarks = buildWatermarkStrategy(getTimestampAssignerFunction());
        return streamExecutionEnvironment.fromSource(source, timestampsAndWatermarks, getName(), getTypeInformation());
    }

}
