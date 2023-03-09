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

package com.ness.flink.config.properties;

import io.lettuce.core.RedisURI;
import java.util.Map;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Khokhlov Pavel
 */
class RedisPropertiesTest {

    @Test
    void shouldGetDefaultValues() {

        RedisProperties properties = RedisProperties.from(ParameterTool.fromMap(Map.of()));
        Assertions.assertEquals("localhost", properties.getHost());
        Assertions.assertEquals(6379, properties.getPort());
        Assertions.assertNull(properties.getPassword());

        // Requires to have password
        properties.setPassword("1");
        RedisURI redisURI = properties.build();

        Assertions.assertEquals("localhost", redisURI.getHost());
        Assertions.assertEquals(6379, redisURI.getPort());
    }
}