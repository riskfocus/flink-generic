/*
 * Copyright 2020-2022 Ness USA, Inc.
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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Factory for properties instantiation. Allows to create individual set of properties by operator's name, which is used
 * to filter them from default properties file {@code (operators.yml)}. Property values are resolved from:
 * <ul>
 *  <li> default values from factory
 *  <li> operators.yml
 *  <li> Environment variables, with transforming names form upper underscore to lowercase with dots (MY_PROP -> my.prop)
 *  <li> Flink {@link ParameterTool}, allowing to pass property prefixed by source name, e.g. {@code -mySource.parallelism 4}
 * </ul>
 * <p> Any vendor specific configuration properties must be passed as is, e.g. {@code max.poll.records},
 * or {@code sourceName.max.poll.records} if ParameterTool argument is used.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
class OperatorPropertiesFactory {

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final String CONFIG_FILE = "/operators.yml";
    private static final String NAME_FIELD = "name";

    /**
     * Read defaults from properties file, overriding with environment variables, overriding with any values from ParameterTool. Usually,
     * ParameterTool passing as argument is created from command line args in main method
     *
     * @param name   source name, to filter records in configuration file
     * @param params overriding values, possibly prefixed with name
     */
    @SneakyThrows
    static <T extends RawProperties<T>> T from(String name, ParameterTool params, Class<T> propertiesClass) {
        Map<String, String> defaults = new HashMap<>();
        defaults.put(NAME_FIELD, name);

        InputStream resourceAsStream = OperatorPropertiesFactory.class.getResourceAsStream(CONFIG_FILE);
        if (resourceAsStream == null) {
            log.info("Configuration file is not found on classpath, continue with defaults: filename={}", CONFIG_FILE);
        } else {
            Map<String, Map<String, String>> fromFile = mapper.readValue(resourceAsStream,
                    new TypeReference<Map<String, Map<String, String>>>() {
                    });
            if (!fromFile.containsKey(name)) {
                log.warn("Config not found: name={}", name);
            } else {
                defaults.putAll(fromFile.get(name));
            }
        }

        defaults.putAll(readEnv());
        defaults.putAll(stripNames(name, params));

        return mapper.convertValue(defaults, propertiesClass).withRawValues(defaults);
    }

    private static Map<String, String> readEnv() {
        return System.getenv().entrySet().stream()
                .map(e -> Map.entry(e.getKey().replace('_', '.').toLowerCase(), e.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Map<String, String> stripNames(String name, ParameterTool params) {
        return params.toMap().entrySet().stream()
                .map(e -> Map.entry(e.getKey().replace(name + ".", ""), e.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

}
