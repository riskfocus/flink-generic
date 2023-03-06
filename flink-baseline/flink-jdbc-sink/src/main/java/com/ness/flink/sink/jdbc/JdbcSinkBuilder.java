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

import com.ness.flink.config.operator.DefaultSink;
import com.ness.flink.sink.jdbc.config.JdbcConnectionOptions;
import com.ness.flink.sink.jdbc.config.JdbcExecutionOptions;
import com.ness.flink.sink.jdbc.config.JdbcOptions;
import com.ness.flink.sink.jdbc.connector.SimpleJdbcConnectionProvider;
import com.ness.flink.sink.jdbc.core.executor.JdbcBatchStatementExecutor;
import com.ness.flink.sink.jdbc.core.executor.JdbcStatementBuilder;
import com.ness.flink.sink.jdbc.core.output.AbstractSinkWriter;
import com.ness.flink.sink.jdbc.core.output.JdbcBatchingOutputFormat;
import com.ness.flink.sink.jdbc.core.output.RecordExtractor;
import com.ness.flink.sink.jdbc.properties.JdbcSinkProperties;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.Sink;

/**
 * Builder for {@link JdbcSink}
 *
 * @author Khokhlov Pavel
 */
@SuperBuilder
@PublicEvolving
public class JdbcSinkBuilder<S> extends DefaultSink<S> {
    private static final long serialVersionUID = 4898245000242257142L;
    private final JdbcSinkProperties jdbcSinkProperties;
    private final String sql;
    private final JdbcStatementBuilder<S> jdbcStatementBuilder;

    @Override
    public Sink<S> build() {
        JdbcConnectionOptions connectionOptions = JdbcOptions.buildJdbcConnectionOptions(jdbcSinkProperties);
        JdbcExecutionOptions executionOptions = JdbcOptions.buildJdbcExecutionOptions(jdbcSinkProperties);
        return sink(sql, jdbcStatementBuilder, executionOptions, connectionOptions);
    }

    private Sink<S> sink(
        String sql,
        JdbcStatementBuilder<S> statementBuilder,
        JdbcExecutionOptions executionOptions,
        JdbcConnectionOptions connectionOptions) {
        return new JdbcSink<>(new JdbcBatchingOutputFormat<>(getName(),
            new SimpleJdbcConnectionProvider(connectionOptions),
            executionOptions,
            context -> JdbcBatchStatementExecutor.simple(sql, statementBuilder, Function.identity(), executionOptions),
            RecordExtractor.identity()
        ));
    }

    @Override
    public Optional<Integer> getParallelism() {
        return Optional.of(jdbcSinkProperties.getParallelism());
    }

    @Override
    public String getName() {
        return jdbcSinkProperties.getName();
    }

    @Slf4j
    @AllArgsConstructor
    private static class JdbcSink<T> implements Sink<T> {
        private static final long serialVersionUID = 362373966141992666L;
        private final AbstractSinkWriter<T> outputFormat;

        @Override
        public org.apache.flink.api.connector.sink2.SinkWriter<T> createWriter(InitContext context) throws IOException {
            outputFormat.open(context);
            return outputFormat;
        }
    }
}
