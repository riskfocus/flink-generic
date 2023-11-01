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
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Khokhlov Pavel
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class OperatorPropertiesUtils {

    /* default */ static Map<String, Object> getEnvVariablesWithFiltration(Set<String> prefixes) {
        Map<String, String> result = new LinkedHashMap<>();
        System.getenv().forEach((envKey, value) -> {
            String readyKey = buildEnvKey(prefixes, envKey);
            if (readyKey != null && !readyKey.isEmpty()) {
                result.put(readyKey, value);
            }
        });
        return convertEnvVariables(result);
    }

    /* default */ static <K, U> void removeNullableEntries(Map<K, U> map) {
        map.entrySet().stream()
                .filter(e -> "NULL".equals(e.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList())
                .forEach(map::remove);
    }

    /* default */ static Map<String, Object> stripPrefixes(Map<String, ?> params, Map<String, Integer> rankedPrefixes) {
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

    /* default */ static String buildEnvKey(Set<String> prefixes, String envKey) {
        for (String prefix : prefixes) {
            if (envKey.startsWith(prefix)) {
                // now we have to remove ENV prefix to match with Class property
                return envKey.replaceFirst(prefix, "");
            }
        }
        return null;
    }

    /* default */ static Map<String, Object> convertEnvVariables(Map<String, String> filteredEnv) {
        return filteredEnv.entrySet().stream().flatMap(e -> {
            String lowerKey = e.getKey().replace('_', '.').toLowerCase(Locale.ENGLISH);
            String camelKey = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, e.getKey());
            return Stream.of(lowerKey, camelKey).distinct().map(k -> Map.entry(k, e.getValue()));
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @AllArgsConstructor
    @Getter
    private static class RankedEntry {
        private final String key;
        private final Object value;
        private final Integer rank;
    }

}
