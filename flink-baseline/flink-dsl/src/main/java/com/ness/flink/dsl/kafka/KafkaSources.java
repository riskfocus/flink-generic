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

package com.ness.flink.dsl.kafka;

import com.ness.flink.dsl.kafka.properties.KafkaSourceProperties;
import com.ness.flink.dsl.properties.PropertiesProvider;
import com.ness.flink.dsl.serialization.JsonDeserializationSchema;
import com.ness.flink.dsl.stream.FlinkSourcedStream;
import java.io.Serializable;
import javax.annotation.Nonnull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Encapsulates Kafka-specific source methods, delegating the implementation details to specific
 * {@link FlinkSourcedStream} implementation.
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@PublicEvolving
public class KafkaSources {

    @Nonnull
    private final StreamExecutionEnvironment env;
    @Nonnull
    private final PropertiesProvider propertiesProvider;

    public static KafkaSources from(@Nonnull StreamExecutionEnvironment env,
                                    @Nonnull PropertiesProvider propertiesProvider) {
        return new KafkaSources(env, propertiesProvider);
    }

    /**
     * Returns an instance of {@link FlinkSourcedStream} with a Kafka source, deserializing incoming values from JSON.
     */
    public <T extends Serializable> FlinkSourcedStream<T> json(@Nonnull String name, @Nonnull Class<T> valueClass) {
        KafkaSourceProperties properties = KafkaSourceProperties.from(name, propertiesProvider);

        return kafkaSourceStream(properties, valueClass,
            KafkaRecordDeserializationSchema.valueOnly(new JsonDeserializationSchema<>(valueClass)));
    }

    /**
     * Returns an instance of {@link FlinkSourcedStream} with a Kafka source, deserializing incoming values from AVRO.
     * Assumed that consumer properties has Schema Registry configuration.
     */
    public <T extends Serializable & SpecificRecord> FlinkSourcedStream<T> avro(@Nonnull String name,
                                                                                @Nonnull Class<T> valueClass) {
        KafkaSourceProperties properties = KafkaSourceProperties.from(name, propertiesProvider);
        KafkaRecordDeserializationSchema<T> schema = KafkaRecordDeserializationSchema.valueOnly(
            ConfluentRegistryAvroDeserializationSchema.forSpecific(valueClass, properties.schemaRegistryUrl()));

        return kafkaSourceStream(properties, valueClass, schema);
    }

    private <T extends Serializable> FlinkSourcedStream<T> kafkaSourceStream(@Nonnull KafkaSourceProperties properties,
                                                                             @Nonnull Class<T> valueClass,
                                                                             @Nonnull KafkaRecordDeserializationSchema<T> schema) {
        KafkaSource<T> source = KafkaSource.<T>builder()
            .setProperties(properties.asJavaProperties())
            .setTopics(properties.getTopic())
            .setStartingOffsets(properties.offsetsInitializer())
            .setDeserializer(schema)
            .build();

        return FlinkSourcedStream.from(
            env.fromSource(source, watermarkStrategy(), properties.getName(), TypeInformation.of(valueClass))
                .name(properties.getName())
                .uid(properties.getName())
                .setParallelism(properties.getParallelism()), propertiesProvider);
    }

    private <T extends Serializable> WatermarkStrategy<T> watermarkStrategy() {
        return WatermarkStrategy.forMonotonousTimestamps(); // TODO pluggable strategies
    }

}