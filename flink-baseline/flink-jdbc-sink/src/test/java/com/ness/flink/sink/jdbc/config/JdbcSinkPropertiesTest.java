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

package com.ness.flink.sink.jdbc.config;

import java.util.Map;
import com.ness.flink.sink.jdbc.properties.JdbcSinkProperties;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Khokhlov Pavel
 */
class JdbcSinkPropertiesTest {

    @Test
    void shouldGetDefaultValues() {
        var jdbcSinkProperties = JdbcSinkProperties.from("test.jdbc.sink", ParameterTool.fromMap(Map.of()));
        Assertions.assertEquals("test.jdbc.sink", jdbcSinkProperties.getName());
        Assertions.assertEquals(2, jdbcSinkProperties.getParallelism());
        Assertions.assertEquals("test-user", jdbcSinkProperties.getUsername());
        Assertions.assertEquals("12345678-a", jdbcSinkProperties.getPassword());
        Assertions.assertEquals("jdbc:mysql://localhost:3306/test?useConfigs=maxPerformance",
            jdbcSinkProperties.getUrl());
    }

}