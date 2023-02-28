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

import com.google.common.annotations.VisibleForTesting;
import com.ness.flink.config.aws.MetricsBuilder;
import com.ness.flink.config.metrics.Metrics;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Provides additional functionality.
 * Skipping messages which cannot be deserialized and Metrics for broken messages (Count).
 *
 * @author Khokhlov Pavel
 */
@Slf4j
final class WithMetricsKafkaDeserialization<T> implements KafkaRecordDeserializationSchema<T> {
    private static final long serialVersionUID = 1L;

    private final String operatorName;
    private final KafkaRecordDeserializationSchema<T> kafkaRecordDeserializationSchema;
    private final boolean skipBrokenMessages;

    /**
     * Number of messages which can't be deserialized
     */
    @VisibleForTesting
    transient Counter brokenMessages;

    private WithMetricsKafkaDeserialization(String operatorName, DeserializationSchema<T> deserializationSchema, boolean skipBrokenMessages) {
        this.operatorName = operatorName;
        this.kafkaRecordDeserializationSchema = KafkaRecordDeserializationSchema.valueOnly(deserializationSchema);
        this.skipBrokenMessages = skipBrokenMessages;
    }

    @VisibleForTesting
    static <T> WithMetricsKafkaDeserialization<T> build(String operatorName,
                                                            DeserializationSchema<T> valueDeserializationSchema,
                                                            boolean skipBrokenMessages) {
        return new WithMetricsKafkaDeserialization<>(operatorName, valueDeserializationSchema, skipBrokenMessages);
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        kafkaRecordDeserializationSchema.open(context);
        MetricGroup metricGroup = context.getMetricGroup();
        brokenMessages = MetricsBuilder.counter(metricGroup, operatorName, Metrics.BROKEN_MESSAGES.getMetricName());
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> message, Collector<T> out) throws IOException {
        try {
            kafkaRecordDeserializationSchema.deserialize(message, out);
        } catch (IOException e) {
            handleDeserializationError(message, e);
        }
    }

    private void handleDeserializationError(ConsumerRecord<byte[], byte[]> message, IOException ioException) throws IOException {
        if (skipBrokenMessages) {
            String errorMessage = String.format("Message was skipped due to deserialization issue: operatorName=%s, topic=%s, partition=%d, offset=%d, timestamp=%d", operatorName, message.topic(), message.partition(), message.offset(), message.timestamp());
            log.warn(errorMessage, ioException);
            brokenMessages.inc();
        } else {
            throw ioException;
        }
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return kafkaRecordDeserializationSchema.getProducedType();
    }
}