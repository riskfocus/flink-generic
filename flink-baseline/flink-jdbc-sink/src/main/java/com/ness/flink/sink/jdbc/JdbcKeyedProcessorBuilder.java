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

package com.ness.flink.sink.jdbc;

import com.ness.flink.config.operator.KeyedProcessorDefinition;
import com.ness.flink.config.properties.OperatorProperties;
import com.ness.flink.sink.jdbc.config.JdbcOptions;
import com.ness.flink.sink.jdbc.connector.SimpleJdbcConnectionProvider;
import com.ness.flink.sink.jdbc.core.executor.JdbcStatementBuilder;
import com.ness.flink.sink.jdbc.core.output.keyed.KeyedJdbcProcessFunction;
import com.ness.flink.sink.jdbc.properties.JdbcSinkProperties;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.function.SerializableFunction;

import javax.annotation.Nullable;

@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@PublicEvolving
public final class JdbcKeyedProcessorBuilder<K, T, U> {
    private final JdbcSinkProperties jdbcSinkProperties;
    private final String sql;

    private final JdbcStatementBuilder<T> jdbcStatementBuilder;
    private final KeySelector<T, K> keySelector;
    private final Class<T> stateClass;
    private final Class<U> returnClass;
    @Nullable
    private final SerializableFunction<T, U> transformerFunction;

    public KeyedProcessorDefinition<K, T, U> buildKeyedProcessor(@NonNull ParameterTool params) {
        OperatorProperties operatorProperties = OperatorProperties.from(jdbcSinkProperties.getName(), params);
        return new KeyedProcessorDefinition<>(operatorProperties, keySelector, createProcessFunction(), returnClass);
    }

    private KeyedJdbcProcessFunction<K, T, U> createProcessFunction() {
        return new KeyedJdbcProcessFunction<>(jdbcSinkProperties.getName(), sql,
            new SimpleJdbcConnectionProvider(JdbcOptions.buildJdbcConnectionOptions(jdbcSinkProperties)),
            JdbcOptions.buildJdbcExecutionOptions(jdbcSinkProperties), jdbcStatementBuilder,
            stateClass, transformerFunction);
    }
}
