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

package com.ness.flink.config.channel.kafka;

import java.io.Serializable;
import com.ness.flink.schema.PojoSerializationSchema;
import lombok.experimental.SuperBuilder;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

/**
 * POJO Sink based
 *
 * @author Khokhlov Pavel
 */
@SuperBuilder
public class PojoRecordSink<S extends Serializable> extends KafkaValueAwareSink<S> {
    @Override
    protected KafkaRecordSerializationSchema<S> getKafkaRecordSerializationSchema() {
        return KafkaSerializationSchemaBuilder.<S>builder()
            .domainClass(domainClass)
            .keyExtractor(keyExtractor)
            .eventTimeExtractor(eventTimeExtractor)
            .topicName(getTopic())
            .serializationSchema(new PojoSerializationSchema<>())
            .metricServiceName(getName())
            .build().build();
    }
}