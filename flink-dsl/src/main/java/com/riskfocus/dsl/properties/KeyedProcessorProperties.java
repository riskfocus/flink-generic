/*
 * Copyright 2020 Risk Focus Inc
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

package com.riskfocus.dsl.properties;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.flink.api.java.utils.ParameterTool;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/**
 * Keyed processor properties for {@link com.riskfocus.dsl.definition.KeyedProcessorDefinition},
 * created by {@link OperatorPropertiesFactory}. Allows to configure any processor-specific properties.
 * <p>
 * NOTE: currently, only name and parallelism are used.
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class KeyedProcessorProperties implements RawProperties<KeyedProcessorProperties> {

    @Getter
    private final String name;
    @Getter
    @Nullable
    private final Integer parallelism;
    private final Map<String, String> rawValues = new HashMap<>();

    /**
     * @inheritDoc
     */
    @SneakyThrows
    public static KeyedProcessorProperties from(String name, ParameterTool params) {
        return OperatorPropertiesFactory.from(name, params, KeyedProcessorProperties.class);
    }

    /**
     * @inheritDoc
     */
    @Override
    public KeyedProcessorProperties withRawValues(Map<String, String> defaults) {
        rawValues.putAll(defaults);

        return this;
    }

}
