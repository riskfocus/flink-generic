//Copyright 2021-2023 Ness Digital Engineering
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
package com.ness.flink.config.channel.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.MetricGroupTest;
import org.apache.flink.util.Collector;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Khokhlov Pavel
 */
class WithMetricsKafkaDeserializationTest {

    private static final String CANNOT_BE_DESERIALIZED = "data";
    private static final byte[] KAFKA_KEY = "key".getBytes();

    DeserializationSchema<Integer> mockSchema = new DeserializationSchema<>() {
        private static final long serialVersionUID = -5610231284231846208L;

        @Override
        public Integer deserialize(byte[] message) throws IOException {
            String messageStr = new String(message);
            if (CANNOT_BE_DESERIALIZED.equals(messageStr)) {
                throw new IOException("Cannot deserialize message");
            } else {
                return Integer.parseInt(messageStr);
            }
        }

        @Override
        public boolean isEndOfStream(Integer nextElement) {
            return false;
        }

        @Override
        public TypeInformation<Integer> getProducedType() {
            return TypeInformation.of(Integer.class);
        }
    };

    DeserializationSchema.InitializationContext mockContext = new DeserializationSchema.InitializationContext() {
        @Override
        public MetricGroup getMetricGroup() {
            return new MetricGroupTest.DummyAbstractMetricGroup(new NoOpMetricRegistry());
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return SimpleUserCodeClassLoader.create(this.getClass().getClassLoader());
        }
    };

    @Test
    void shouldSkipDeserializedMessage() throws Exception {

        WithMetricsKafkaDeserialization<Integer> schema = WithMetricsKafkaDeserialization.build("test", mockSchema, true);
        schema.open(mockContext);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test", 0, 1,
                KAFKA_KEY, CANNOT_BE_DESERIALIZED.getBytes());
        ArrayListCollector<Integer> out = new ArrayListCollector<>();
        schema.deserialize(record, out);
        Assertions.assertEquals(0, out.data.size(), "This message cannot be deserialized");
        Assertions.assertEquals(1, schema.brokenMessages.getCount());
    }

    @Test
    void shouldNotSkipDeserializedMessage() throws Exception {
        WithMetricsKafkaDeserialization<Integer> schema = WithMetricsKafkaDeserialization.build("test", mockSchema, false);
        schema.open(mockContext);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test", 0, 1,
                KAFKA_KEY, CANNOT_BE_DESERIALIZED.getBytes());
        ArrayListCollector<Integer> out = new ArrayListCollector<>();

        Exception exception = assertThrows(IOException.class, () -> schema.deserialize(record, out));
        Assertions.assertEquals(0, schema.brokenMessages.getCount());
        Assertions.assertEquals(0, out.data.size(), "This message cannot be deserialized");
        Assertions.assertEquals("Cannot deserialize message", exception.getMessage());
    }

    @Test
    void shouldDeserializedMessage() throws Exception {
        WithMetricsKafkaDeserialization<Integer> schema = WithMetricsKafkaDeserialization.build("test", mockSchema, true);
        schema.open(mockContext);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test", 0, 1,
                KAFKA_KEY, "2".getBytes());
        ArrayListCollector<Integer> out = new ArrayListCollector<>();

        schema.deserialize(record, out);
        Assertions.assertEquals(1, out.data.size(), "This message has to be deserialized");
        Assertions.assertEquals(2, out.data.get(0));
        Assertions.assertEquals(0, schema.brokenMessages.getCount());
    }


    static class ArrayListCollector<T> implements Collector<T> {
        private final List<T> data = new ArrayList<>();

        @Override
        public void collect(T record) {
            data.add(record);
        }

        @Override
        public void close() {

        }
    }

}