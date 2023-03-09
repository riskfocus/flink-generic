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


import com.google.common.base.CaseFormat;
import com.ness.flink.config.aws.KdaPropertiesManager;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;


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
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@SuppressWarnings("PMD.TooManyMethods")
public class OperatorPropertiesFactory {

    /**
     * Default pipeline configuration file
     */
    public static final String DEFAULT_CONFIG_FILE = "/application.yml";
    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final String NAME_FIELD = "name";
    private static final String PLACEHOLDER_PATTERN_STRING = "\\$\\{(.+?)\\}";
    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile(PLACEHOLDER_PATTERN_STRING);

    /**
     * Read defaults from properties file, overriding with environment variables, overriding with any values from
     * ParameterTool. Usually, ParameterTool passing as argument is created from command line args in main method
     *
     * @param name   source name, to filter records in configuration file
     * @param params overriding values, possibly prefixed with name
     * @param propertiesClass Class which describes required properties
     */
    public static <T extends RawProperties<T>> T from(String name, ParameterTool params, Class<T> propertiesClass) {
        return from(name, null, params, propertiesClass, DEFAULT_CONFIG_FILE);
    }

    /**
     * Read defaults from properties file, overriding with environment variables, overriding with any values from
     * ParameterTool. Usually, ParameterTool passing as argument is created from command line args in main method
     *
     * @param name   source name, to filter records in configuration file
     * @param sharedName  Shared source name, which will be used in case of original property name wasn't found
     * @param params overriding values, possibly prefixed with name
     * @param propertiesClass Class which describes required properties
     * @param configFile path to configuration YML-file
     */
    public static <T extends RawProperties<T>> T from(String name, String sharedName, ParameterTool params, Class<T> propertiesClass,
                                               String configFile) {
        PropertiesMaps propertiesMaps = propertiesMaps(name, sharedName, params, configFile);
        return MAPPER.convertValue(propertiesMaps.getDefaults(), propertiesClass).withRawValues(Collections.unmodifiableMap(propertiesMaps.getRawValues()));
    }

    public static <T> T genericProperties(String name, ParameterTool params, Class<T> propertiesClass, String configFile) {
        return genericProperties(name, null, params, propertiesClass, configFile);
    }

    public static <T> T genericProperties(String name, String sharedName, ParameterTool params, Class<T> propertiesClass, String configFile) {
        PropertiesMaps propertiesMaps = propertiesMaps(name, sharedName, params, configFile);
        return MAPPER.convertValue(propertiesMaps.getDefaults(), propertiesClass);
    }

    /**
     * Read defaults from properties file, overriding with environment variables, overriding with any values from
     * ParameterTool. Usually, ParameterTool passing as an argument is created from command line args in main method
     * To support AWS KDA, reads provided properties as well, with lower precedence
     *
     * @param name source name, to filter records in configuration file
     * @param sharedName shared or common name in configuration file for this propertiesClass
     * @param params overriding values, possible prefixed with name
     * @return Configuration
     */
    private static PropertiesMaps propertiesMaps(String name, String sharedName, ParameterTool params, String configFile) {
        Map<String, Object> defaults = new HashMap<>();
        defaults.put(NAME_FIELD, name);
        try (InputStream resourceAsStream = OperatorPropertiesFactory.class.getResourceAsStream(configFile)) {
            Map<String, Object> fromFile = MAPPER.readValue(resourceAsStream, Map.class);
            // 1. get settings from application.yml file
            if (sharedName != null) {
                Map<String, Object> sharedData = (Map<String, Object>) fromFile.get(sharedName);
                if (sharedData != null) {
                    defaults.putAll(sharedData);
                }
            }
            Object configuration = fromFile.get(name);
            if (configuration == null) {
                log.warn("Config not found in YAML file: name={}", name);
            } else if (configuration instanceof Map) {
                defaults.putAll((Map<String, Object>) configuration);
            }

        } catch (IOException e) {
            log.warn("Configuration file not found in classpath, continue with defaults: filename={}", configFile);
        }
        Map<String, Integer> prefixes = new LinkedHashMap<>();
        prefixes.put(name, 0);
        if (sharedName != null && !sharedName.equals(name)) {
            prefixes.put(sharedName, 1);
        }
        // Build prefixes for filtering out ENV variables (we should use only variables which are in name scope)
        Set<String> camelPrefixes = prefixes.keySet().stream()
                .map(String::toUpperCase)
                .map(p -> {
                    // Building ENV based keys
                    p = p.replace(".", "_");
                    if (p.endsWith("_")) {
                        return p;
                    } else {
                        return p + "_";
                    }
                }).collect(Collectors.toSet());
        defaults.putAll(stripPrefixes(getEnvVariablesWithFiltration(camelPrefixes), prefixes));

        KdaPropertiesManager.mergeWithKdaProperties(aws -> defaults.putAll(stripPrefixes(aws, prefixes)));
        // command line params
        Map<String, Object> stripedPrefixes = stripPrefixes(params.toMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        e -> "__NO_VALUE_KEY".equals(e.getValue()) ? "" : e.getValue())), prefixes);
        defaults.putAll(stripedPrefixes);

        defaults.putAll(evaluatePlaceHolders(defaults));

        Map<String, String> rawValues = defaults.entrySet().stream()
                .filter(e -> !(e.getValue() instanceof Map) && !(e.getValue() instanceof List))
                .collect(Collectors.toMap(Map.Entry::getKey, e -> String.valueOf(e.getValue())));

        removeNullableEntries(defaults);
        removeNullableEntries(rawValues);

        return new PropertiesMaps(defaults, rawValues);

    }

    private static <K, U> void removeNullableEntries(Map<K, U> map) {
        map.entrySet().stream()
                .filter(e -> "NULL".equals(e.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList())
                .forEach(map::remove);
    }

    private static Map<String, Object> evaluatePlaceHolders(Map<String, Object> params) {
        Map<String, Object> replacedMap = applyReplacement(params, params);
        replacedMap.putAll(params.entrySet().stream()
                .filter(e -> e.getValue() instanceof Map)
                .map(e -> {
                    Map<String, Object> subMap = (Map<String, Object>) e.getValue();
                    Map<String, Object> replacedSubMap = applyReplacement(subMap, params);
                    subMap.putAll(replacedSubMap);
                    return replacedSubMap.isEmpty() ? null : Map.entry(e.getKey(), subMap);

                }).filter(Objects::nonNull)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

        return replacedMap;
    }

    @SuppressWarnings("PMD.GuardLogStatement")
    private static Map<String, Object> applyReplacement(Map<String, Object> forReplacement, Map<String, Object> params) {
        return forReplacement.entrySet().stream()
                .filter(e -> e.getValue() instanceof String)
                .filter(e -> PLACEHOLDER_PATTERN.matcher((String) e.getValue()).find())
                .map(e -> {
                    StringBuilder result = new StringBuilder();
                    Matcher matcher = PLACEHOLDER_PATTERN.matcher((String) e.getValue());
                    while (matcher.find()) {
                        String newValue = null;
                        String placeholder = matcher.group(1);
                        if (params.containsKey(placeholder)) {
                            newValue = (String) params.get(placeholder);
                        } else if (System.getenv().containsKey(placeholder)) {
                            newValue = System.getenv(placeholder);
                        }
                        if (newValue == null) {
                            log.error("Missing value: key={}, value={}, placeholder={}", e.getKey(), e.getValue(), placeholder);
                            return null;
                        } else {
                            matcher.appendReplacement(result, newValue);
                        }
                    }
                    matcher.appendTail(result);
                    return Map.entry(e.getKey(), result.toString());

                })
                .filter(Objects::nonNull).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Map<String, Object> getEnvVariablesWithFiltration(Set<String> prefixes) {
        Map<String, String> result = new LinkedHashMap<>();
        System.getenv().forEach((envKey, value) -> {
            String readyKey = buildEnvKey(prefixes, envKey);
            if (readyKey != null && !readyKey.isEmpty()) {
                result.put(readyKey, value);
            }
        });
        return convertEnvVariables(result);
    }

    private static String buildEnvKey(Set<String> prefixes, String envKey) {
        for (String prefix : prefixes) {
            if (envKey.startsWith(prefix)) {
                // now we have to remove ENV prefix to match with Class property
                return envKey.replaceFirst(prefix, "");
            }
        }
        return null;
    }

    private static Map<String, Object> convertEnvVariables(Map<String, String> filteredEnv) {
        return filteredEnv.entrySet().stream().flatMap(e -> {
            String lowerKey = e.getKey().replace('_', '.').toLowerCase(Locale.ENGLISH);
            String camelKey = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, e.getKey());
            return Stream.of(lowerKey, camelKey).distinct().map(k -> Map.entry(k, e.getValue()));
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Map<String, Object> stripPrefixes(Map<String, ?> params, Map<String, Integer> rankedPrefixes) {
        return params.entrySet().stream()
                .map(e -> rankedPrefixes.entrySet().stream().filter(prefixEntry ->
                                e.getKey().startsWith(prefixEntry.getKey() + ".")
                        ).findFirst().map(prefixEntry ->
                                new RankedEntry(e.getKey().replace(prefixEntry.getKey() + ".", ""),
                                        e.getValue(), prefixEntry.getValue())
                        )
                        .orElse(new RankedEntry(e.getKey(), e.getValue(), Integer.MAX_VALUE))).collect(Collectors.toMap(RankedEntry::getKey, Function.identity(),
                        BinaryOperator.minBy(Comparator.comparing(RankedEntry::getRank))))
                .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getValue()));
    }

    @AllArgsConstructor
    @Getter
    private static class RankedEntry {
        private final String key;
        private final Object value;
        private final Integer rank;
    }

    @AllArgsConstructor
    @Getter
    private static class PropertiesMaps {
        private final Map<String, Object> defaults;
        private final Map<String, String> rawValues;

    }

}
