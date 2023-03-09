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

import com.ness.flink.config.aws.MetricsBuilder;
import com.ness.flink.config.channel.EventTimeExtractor;
import com.ness.flink.config.channel.KeyExtractor;
import java.io.Serializable;
import com.ness.flink.config.metrics.Metrics;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.MetricGroup;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Kafka Serialization Builder
 * Provides Metrics
 * @author Khokhlov Pavel
 */
@SuperBuilder
public class KafkaSerializationSchemaBuilder<S extends Serializable> implements Serializable {

    private static final long serialVersionUID = -9098469328017941187L;

    @NonNull
    protected final Class<S> domainClass;
    @NonNull
    protected final String topicName;
    @NonNull
    private final String metricServiceName;
    @NonNull
    private final KeyExtractor<S> keyExtractor;

    @NonNull
    private final SerializationSchema<S> serializationSchema;

    private final EventTimeExtractor<S> eventTimeExtractor;

    public KafkaRecordSerializationSchema<S> build() {
        return new KafkaRecordSerializationSchema<>() {
            private static final long serialVersionUID = 4317638737998593333L;
            private transient DropwizardHistogramWrapper histogram;
            private transient Long e2eTimestamp;

            @Override
            @SuppressWarnings("PMD.NullAssignment")
            public ProducerRecord<byte[], byte[]> serialize(S value, KafkaSinkContext context, Long timestamp) {
                Long eventTime = timestamp;
                if (eventTimeExtractor != null) {
                    eventTime = eventTimeExtractor.getTimestamp(value);
                }
                byte[] serializedValue = serializationSchema.serialize(value);
                byte[] serializedKey = keyExtractor.getKey(value);
                if (eventTime != null) {
                    e2eTimestamp = System.currentTimeMillis() - eventTime;
                    histogram.update(e2eTimestamp);
                }
                return new ProducerRecord<>(topicName, null, timestamp == null || timestamp < 0L ? null : timestamp, serializedKey, serializedValue);
            }

            @Override
            public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
                KafkaRecordSerializationSchema.super.open(context, sinkContext);
                MetricGroup metricGroup = context.getMetricGroup();
                String e2eLatency = Metrics.END_TO_END_LATENCY.getMetricName();
                MetricsBuilder.gauge(metricGroup, metricServiceName, e2eLatency, () -> e2eTimestamp);
                histogram = MetricsBuilder.histogram(metricGroup, metricServiceName, e2eLatency);
            }
        };
    }
}
