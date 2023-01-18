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
import java.io.Serializable;

/**
 * Any Flink Operator properties
 *
 * @author Khokhlov Pavel
 */
@Getter
@Setter
@Slf4j
@ToString
public class OperatorProperties implements Serializable {
    private static final long serialVersionUID = -2176914485613750471L;

    private String name;
    private Integer parallelism;

    public static OperatorProperties from(@NonNull String name, @NonNull ParameterTool params) {
        OperatorProperties properties = from(name, params, OperatorPropertiesFactory.DEFAULT_CONFIG_FILE);
        log.info("Build parameters: operatorProperties={}", properties);
        return properties;
    }

    static OperatorProperties from(@NonNull String name, @NonNull ParameterTool parameterTool,
                                   @NonNull String ymlConfigFile) {
        return OperatorPropertiesFactory
                .genericProperties(name, parameterTool, OperatorProperties.class, ymlConfigFile);
    }
}
