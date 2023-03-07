/*
 * Copyright 2020-2023 Ness USA, Inc.
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

package com.ness.flink.config.channel.kafka.confluent;


import com.ness.flink.config.channel.kafka.KafkaAwareSink;
import com.ness.flink.config.channel.kafka.KafkaSerializationSchemaBuilder;
import lombok.experimental.SuperBuilder;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.formats.avro.RegistryAvroSerializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;

/**
 * Confluent Kafka Sink AVRO {@link SpecificRecordBase} based
 *
 * @author Khokhlov Pavel
 */
@SuperBuilder
public final class ConfluentAvroSpecificRecordSink<S extends SpecificRecordBase> extends KafkaAwareSink<S> {

    @Override
    protected KafkaRecordSerializationSchema<S> getKafkaRecordSerializationSchema() {
        return KafkaSerializationSchemaBuilder.<S>builder()
                .domainClass(domainClass)
                .keyExtractor(keyExtractor)
                .eventTimeExtractor(eventTimeExtractor)
                .topicName(getTopic())
                .serializationSchema(buildSerializationSchema())
                .metricServiceName(getName())
                .build().build();
    }

    private RegistryAvroSerializationSchema<S> buildSerializationSchema() {
        return ConfluentRegistryAvroSerializationSchema.forSpecific(domainClass, getTopic() + "-value",
                kafkaProducerProperties.getConfluentSchemaRegistry(),
                kafkaProducerProperties.getConfluentRegistryConfigs());
    }
}