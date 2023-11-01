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

package com.ness.flink.dsl.kafka.properties;

import com.ness.flink.dsl.kafka.properties.KafkaSourceProperties.Offsets;
import com.ness.flink.dsl.properties.PropertiesProvider;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class KafkaSourcePropertiesTest {

    @Test
    void shouldGetDefaultValuesFromFile() {
        KafkaSourceProperties properties = KafkaSourceProperties.from("default.source",
            PropertiesProvider.from(ParameterTool.fromMap(Map.of())));
        Assertions.assertEquals(4, properties.getParallelism());
        Assertions.assertEquals("default.source", properties.getName());
        Assertions.assertEquals("http://localhost:8085", properties.schemaRegistryUrl());
        Assertions.assertEquals(Offsets.COMMITTED, properties.getOffsets());

        OffsetsInitializer offsetsInitializer = properties.offsetsInitializer();
        Assertions.assertEquals(OffsetResetStrategy.EARLIEST, offsetsInitializer.getAutoOffsetResetStrategy());
        Assertions.assertNull(properties.getTimestamp());

        Properties consumerProperties = properties.asJavaProperties();
        Assertions.assertEquals("localhost:19093",
            consumerProperties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        Assertions.assertEquals("testGroup", consumerProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
        Assertions.assertEquals("1", consumerProperties.getProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG));
        Assertions.assertEquals("500", consumerProperties.getProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG));
        Assertions.assertEquals("read_committed",
            consumerProperties.getProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG));
        Assertions.assertEquals("true", consumerProperties.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        Assertions.assertEquals("5000",
            consumerProperties.getProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG));
    }

}