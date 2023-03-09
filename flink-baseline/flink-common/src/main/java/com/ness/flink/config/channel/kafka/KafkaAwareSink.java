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

import com.ness.flink.config.channel.EventTimeExtractor;
import com.ness.flink.config.channel.KeyExtractor;
import com.ness.flink.config.operator.DefaultSink;
import com.ness.flink.config.properties.KafkaProducerProperties;
import lombok.experimental.SuperBuilder;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;

import java.io.Serializable;
import java.util.Optional;
import java.util.Properties;

/**
 *
 * @author Khokhlov Pavel
 */
@SuperBuilder
public abstract class KafkaAwareSink<S extends Serializable> extends DefaultSink<S> {
    protected final KafkaProducerProperties kafkaProducerProperties;
    protected final Class<S> domainClass;
    protected final KeyExtractor<S> keyExtractor;
    protected final EventTimeExtractor<S> eventTimeExtractor;

    protected Properties producerProps() {
        return kafkaProducerProperties.getProducerProperties();
    }

    protected String getTopic() {
        return kafkaProducerProperties.getTopic();
    }

    @Override
    public Optional<Integer> getParallelism() {
        return Optional.ofNullable(kafkaProducerProperties.getParallelism());
    }

    protected abstract KafkaRecordSerializationSchema<S> getKafkaRecordSerializationSchema();

    @Override
    public final Sink<S> build() {
        KafkaSinkBuilder<S> builder = KafkaSink.<S>builder()
                .setDeliverGuarantee(kafkaProducerProperties.getDeliveryGuarantee())
                .setKafkaProducerConfig(producerProps())
                .setRecordSerializer(getKafkaRecordSerializationSchema());
        kafkaProducerProperties.buildTransactionIdPrefix(getName()).ifPresent(builder::setTransactionalIdPrefix);
        return builder.build();
    }

}