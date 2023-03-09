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

import java.util.Map;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Khokhlov Pavel
 */
class OperatorPropertiesTest {
    private static final String TEST_CONFIG = "/application-test.yml";

    @Test
    void shouldGetDefaultValues() {
        var operatorProperties = OperatorProperties.from("defaultSource", ParameterTool.fromMap(Map.of()));
        Assertions.assertEquals("defaultSource", operatorProperties.getName());
        Assertions.assertNull(operatorProperties.getParallelism());
    }

    @Test
    void shouldGetTestConfiguration() {
        var operatorProperties = OperatorProperties.from("test.operator", ParameterTool.fromMap(Map.of()), TEST_CONFIG);
        Assertions.assertEquals("test.operator", operatorProperties.getName());
        Assertions.assertEquals(4, operatorProperties.getParallelism());
    }


}