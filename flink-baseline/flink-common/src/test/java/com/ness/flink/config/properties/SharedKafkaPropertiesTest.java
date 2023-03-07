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
package com.ness.flink.config.properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.ness.flink.config.properties.OperatorPropertiesFactory.DEFAULT_CONFIG_FILE;

class SharedKafkaPropertiesTest {
    @Test
    void shouldGetDefaultValues() {
        SharedKafkaProperties properties = SharedKafkaProperties.from("defaultSource",
                ParameterTool.fromMap(Map.of()), DEFAULT_CONFIG_FILE);

        Map<String, String> rawValues = properties.getRawValues();
        Assertions.assertEquals("defaultSource", rawValues.get("name"));
        Assertions.assertEquals("localhost:29092", rawValues.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        Assertions.assertEquals("http://localhost:8085", rawValues.get("schema.registry.url"));
        Assertions.assertEquals("URL", rawValues.get("schema.registry.basic.auth.credentials.source"));
        Assertions.assertEquals("user:pwd", rawValues.get("schema.registry.basic.auth.user.info"));
    }
    @Test
    void shouldGetValues() {
        SharedKafkaProperties properties = SharedKafkaProperties.from("kafka.consumer",
                ParameterTool.fromMap(Map.of()), "/application-test.yml");
        Map<String, String> rawValues = properties.getRawValues();

        Assertions.assertEquals("kafka.consumer", rawValues.get("name"));
        Assertions.assertEquals("localhost:9092", rawValues.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));

        Assertions.assertEquals("http://localhost:8085", rawValues.get("schema.registry.url"));
        Assertions.assertEquals("USER_INFO", rawValues.get("schema.registry.basic.auth.credentials.source"));
        Assertions.assertEquals("12121:233232", rawValues.get("schema.registry.basic.auth.user.info"));
    }
}