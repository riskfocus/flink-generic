/*
 * Copyright 2020-2022 Ness USA, Inc.
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

package com.ness.flink.dsl.definition.kafka;

import com.ness.flink.dsl.definition.SimpleDefinition;
import com.ness.flink.dsl.definition.SourceDefinition;
import com.ness.flink.dsl.properties.KafkaConsumerProperties;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import java.io.Serializable;
import java.util.Optional;

/**
 * Creates Flink {@link SourceFunction} for Kafka. Requires serializers for keys and values,
 * to decouple event type from any interface. Value serializer might be a static method in
 * entity class, similar to how Avro/Protobuf provide serialization.
 *
 * @param <T> event type to send to Kafka
 */
public class CommonKafkaSource<T> implements SourceDefinition<T>, SimpleDefinition, Serializable {

    private final KafkaConsumerProperties properties;
    private final KafkaDeserializationSchema<T> schema;

    public CommonKafkaSource(KafkaConsumerProperties properties,
                             KafkaDeserializationSchema<T> schema) {
        this.schema = schema;
        this.properties = properties;
    }

    @Override
    public SourceFunction<T> buildSource() {
        return new FlinkKafkaConsumer<>(properties.getTopic(), schema, properties.getConsumerProperties());
    }

    @Override
    public String getName() {
        return properties.getName();
    }

    @Override
    public Optional<Integer> getParallelism() {
        return Optional.ofNullable(properties.getParallelism());
    }

}
