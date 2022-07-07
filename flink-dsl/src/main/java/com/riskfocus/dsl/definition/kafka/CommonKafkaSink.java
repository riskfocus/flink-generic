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

package com.riskfocus.dsl.definition.kafka;

import com.riskfocus.dsl.definition.SimpleDefinition;
import com.riskfocus.dsl.definition.SinkDefinition;
import com.riskfocus.dsl.properties.KafkaProducerProperties;
import com.riskfocus.dsl.serialization.KafkaSinkSerializer;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Optional;

/**
 * Creates Flink {@link SinkFunction} for Kafka. Requires serializers for keys and values,
 * to decouple event type from any interface. Value serializer might be a static method in
 * entity class, similar to how Avro/Protobuf provide serialization.
 * <p>
 * NOTE: sets default producer semantic to AT_LEAST_ONCE, if none provided.
 *
 * @param <T> event type to send to Kafka
 */
public class CommonKafkaSink<T> implements SinkDefinition<T>, SimpleDefinition {

    private final KafkaProducerProperties properties;
    private final KafkaSinkSerializer<T> keySerializer;
    private final KafkaSinkSerializer<T> valueSerializer;

    public CommonKafkaSink(KafkaProducerProperties properties,
                           KafkaSinkSerializer<T> keySerializer,
                           KafkaSinkSerializer<T> valueSerializer) {
        this.properties = properties;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    /**
     * @inheritDoc
     */
    @Override
    public SinkFunction<T> buildSink() {
        KafkaSerializationSchema<T> schema = (value, timestamp) ->
                new ProducerRecord<>(properties.getTopic(), keySerializer.apply(value), valueSerializer.apply(value));
        FlinkKafkaProducer<T> sinkFunction = new FlinkKafkaProducer<>(properties.getTopic(), schema,
                properties.getProducerProperties(), properties.getSemantic().orElse(FlinkKafkaProducer.Semantic.AT_LEAST_ONCE));
        sinkFunction.setWriteTimestampToKafka(true);

        return sinkFunction;
    }

    /**
     * @inheritDoc
     */
    @Override
    public String getName() {
        return properties.getName();
    }

    /**
     * @inheritDoc
     */
    @Override
    public Optional<Integer> getParallelism() {
        return Optional.ofNullable(properties.getParallelism());
    }

}
