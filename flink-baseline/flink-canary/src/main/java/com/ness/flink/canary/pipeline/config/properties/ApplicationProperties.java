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

package com.ness.flink.canary.pipeline.config.properties;

import static com.ness.flink.config.properties.OperatorPropertiesFactory.DEFAULT_CONFIG_FILE;

import com.google.common.annotations.VisibleForTesting;
import com.ness.flink.config.properties.OperatorPropertiesFactory;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

@Getter
@Setter
@Slf4j
@ToString
public class ApplicationProperties {
    private static final String NAME = "application";

    private String topic;

    public static ApplicationProperties from(@NonNull ParameterTool parameterTool) {
        ApplicationProperties properties = from(NAME, parameterTool, DEFAULT_CONFIG_FILE);
        log.info("Build parameters: applicationProperties={}", properties);
        return properties;
    }

    @VisibleForTesting
    static ApplicationProperties from(@NonNull String name, @NonNull ParameterTool parameterTool,
        @NonNull String ymlConfigFile) {
        return OperatorPropertiesFactory
            .genericProperties(name, parameterTool, ApplicationProperties.class, ymlConfigFile);
    }

}
