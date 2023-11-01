/*
 * Copyright 2021-2024 Ness Digital Engineering
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.ness.flink.dsl.properties;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.CaseFormat;
import com.ness.flink.dsl.StreamBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

/**
 * Encapsulates all the properties from different sources and allows to create property instances, resolved by name.
 * Properties are represented by {@code Map<String, Object>} and are resolved with sources precedence:
 * <ul>
 *     <li>{@code application.yml} file in each module resources folder</li>
 *     <li>Environment variables converted to prefixed camelcase, e.g. APP_INPUT_TYPE -> app.inputType</li>
 *     <li>Externally stored properties, such as KDA config, or Secret Manager/Vault stored config</li>
 *     <li>Flink {@link ParameterTool}, carrying properties from command line arguments and other sources</li>
 * </ul>
 * Assumed that properties are organized by prefix, and inside this prefix each property name maps to a Java class field
 * name (without prefix), e.g. properties {@code application.runMode} and {@code application.windowType} are mapped to
 * fields {@code runMode} and {@code windowType} for a name {@code application}. Note that all properties are resolved
 * in constructor, and there is only one instance of {@link PropertiesProvider} exists during runtime. In the DSL this
 * is achieved by encapsulating it in {@link StreamBuilder}.
 * <p>
 * The main purpose of this class is to unify properties instantiation, by converting map of properties to a class
 * instance using {@link ObjectMapper#convertValue(Object, Class)} method.
 */
@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@SuppressWarnings({"PMD.TooManyMethods", "PMD.AvoidFieldNameMatchingMethodName", "PMD.GuardLogStatement"})
@Internal
public final class PropertiesProvider {

    private static final String DEFAULT_CONFIG_FILE = "/application.yml";
    /**
     * Copy from Flink {@link org.apache.flink.api.java.utils.AbstractParameterTool}
     */
    private static final String NO_VALUE_KEY = "__NO_VALUE_KEY";
    private static final char UNDERSCORE = '_';
    private static final char DELIMITER = '.';
    /**
     * Each property class must have a field with this name, so it is populated from configuration sources
     */
    private static final String NAME = "name";

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory())
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /**
     * Each property value has the highest precedence value
     */
    private final Map<String, Object> properties;

    /**
     * Provides an instance with all properties resolved from sources, without external sources.
     */
    public static PropertiesProvider from(ParameterTool parameterTool) {
        Map<String, Object> properties = fromSources(parameterTool);

        return new PropertiesProvider(properties);
    }

    /**
     * Provides an instance with all properties resolved from sources, with external sources. Order or sources defines
     * precedence.
     */
    public static PropertiesProvider from(ParameterTool parameterTool, ExternalPropertiesProvider... providers) {
        Map<String, Object> properties = fromSources(parameterTool, providers);

        return new PropertiesProvider(properties);
    }

    /**
     * Creates an instance of properties class populated with values from all the source, filtered by prefix(es). If
     * multiple prefixes presented, their order defines precedence.
     */
    public <T extends RawProperties<T>> T properties(String prefix, Class<T> propertiesClass) {
        Map<String, Object> properties = new HashMap<>(filtered(prefix));

        return MAPPER.convertValue(properties, propertiesClass)
            .withRawValues(properties.entrySet().stream()
                .map(e -> Map.entry(e.getKey(), e.getValue().toString()))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue)));
    }

    public <T extends RawProperties<T>> T namedProperties(String name,
                                                          Class<T> propertiesClass,
                                                          String... prefixes) {
        Map<String, Object> properties = new HashMap<>();
        for (String prefix : prefixes) {
            properties.putAll(filtered(prefix));
        }

        properties.putAll(filtered(name));
        properties.put(NAME, name);

        return MAPPER.convertValue(properties, propertiesClass)
            .withRawValues(properties.entrySet().stream()
                .map(e -> Map.entry(e.getKey(), e.getValue().toString()))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue)));
    }

    /**
     * Creates an instance of Flink job parameters with values from all sources, making them accessible during Flink
     * function initialization.
     *
     * @see org.apache.flink.api.common.functions.RichFunction#open(Configuration)
     */
    public GlobalJobParameters parameters() {
        return ParameterTool.fromMap(properties.entrySet().stream()
            .map(e -> Map.entry(e.getKey(), e.getValue().toString()))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue)));
    }

    /**
     * Defines source order, from the lowest to the highest precedence.
     */
    private static Map<String, Object> fromSources(ParameterTool parameterTool,
                                                   ExternalPropertiesProvider... providers) {
        Map<String, Object> properties = new HashMap<>();

        properties.putAll(fromFile());
        properties.putAll(fromEnvironment());
        for (ExternalPropertiesProvider provider : providers) {
            properties.putAll(provider.properties());
        }
        properties.putAll(fromParameters(parameterTool));

        return properties;
    }

    private static Map<String, Object> fromFile() {
        Map<String, Object> properties = new HashMap<>();

        try (InputStream resourceAsStream = PropertiesProvider.class.getResourceAsStream(DEFAULT_CONFIG_FILE)) {
            Map<String, Map<String, Object>> byPrefix = MAPPER.readValue(resourceAsStream,
                new TypeReference<>() {
                });
            byPrefix.entrySet().stream()
                .flatMap(e -> e.getValue().entrySet().stream()
                    .map(en -> Map.entry(e.getKey() + DELIMITER + en.getKey(), en.getValue())))
                .forEach(e -> properties.put(e.getKey(), e.getValue()));
        } catch (IOException e) {
            log.warn("Exception on reading from file: filename={}, error={}", DEFAULT_CONFIG_FILE, e.getMessage());
        }

        return properties;
    }

    private static Map<String, String> fromEnvironment() {
        return System.getenv().entrySet().stream()
            .map(PropertiesProvider::convertedKey)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Converts given entry key from upper underscore format to prefixed camelcase. Note that there is no filtering, so
     * any given value is converted, even if it's not desirable (e.g. env variable {@code JAVA_HOME} will become
     * {@code java.home}).
     */
    private static Entry<String, String> convertedKey(Entry<String, String> entry) {
        int prefixPosition = entry.getKey().indexOf(UNDERSCORE);
        if (prefixPosition < 0) {
            return entry;
        }

        String prefix = entry.getKey().substring(0, prefixPosition);
        String name = entry.getKey().substring(1 + prefixPosition);
        return Map.entry(
            CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, prefix) + DELIMITER +
                CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name),
            entry.getValue()
        );
    }

    private static Map<String, String> fromParameters(ParameterTool parameterTool) {
        return parameterTool.toMap().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey,
                e -> NO_VALUE_KEY.equals(e.getValue()) ? "" : e.getValue()));
    }

    /**
     * Extracts only properties with a given prefix, and return them <b>with prefix stripped</b>.
     */
    private Map<String, Object> filtered(String prefix) {
        return properties.entrySet().stream()
            .filter(e -> e.getKey().startsWith(prefix))
            .map(e -> withoutPrefix(e, prefix))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Entry<String, Object> withoutPrefix(Entry<String, Object> entry, String prefix) {
        return Map.entry(entry.getKey().replaceAll(prefix + DELIMITER, ""), entry.getValue());
    }

}