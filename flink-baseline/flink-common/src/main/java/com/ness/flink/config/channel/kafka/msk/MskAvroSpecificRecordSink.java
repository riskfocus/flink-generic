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

package com.ness.flink.config.channel.kafka.msk;

import com.amazonaws.services.schemaregistry.flink.avro.GlueSchemaRegistryAvroSerializationSchema;
import com.ness.flink.config.channel.kafka.KafkaAwareSink;
import com.ness.flink.config.channel.kafka.KafkaSerializationSchemaBuilder;
import com.ness.flink.config.properties.AwsProperties;
import lombok.experimental.SuperBuilder;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.formats.avro.RegistryAvroSerializationSchema;

/**
 * @author Khokhlov Pavel
 */
@SuperBuilder
public final class MskAvroSpecificRecordSink<S extends SpecificRecordBase> extends KafkaAwareSink<S> {
    private final AwsProperties awsProperties;

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
        return GlueSchemaRegistryAvroSerializationSchema.forSpecific(domainClass, getTopic(),
                awsProperties.getAwsGlueSchemaConfig(getTopic()));
    }
}