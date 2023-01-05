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

package com.ness.flink.config.properties;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Shared Kafka Properties
 * Like: bootstrap & schema configuration
 *
 * @author Khokhlov Pavel
 */
@Slf4j
@Getter
@Setter
public class SharedKafkaProperties implements RawProperties<SharedKafkaProperties> {
    private static final long serialVersionUID = -5869618107831628206L;
    private static final String SHARED_PROPERTY_NAME = "kafka";

    @ToString.Exclude
    private Map<String, String> rawValues = new LinkedHashMap<>();

    static SharedKafkaProperties from(@NonNull String name, @NonNull ParameterTool parameterTool,
                                      @NonNull String ymlConfigFile) {
        return OperatorPropertiesFactory.from(name, SHARED_PROPERTY_NAME, parameterTool,
                SharedKafkaProperties.class, ymlConfigFile);
    }

    @Override
    public SharedKafkaProperties withRawValues(Map<String, String> defaults) {
        rawValues.putAll(defaults);
        return this;
    }
}
