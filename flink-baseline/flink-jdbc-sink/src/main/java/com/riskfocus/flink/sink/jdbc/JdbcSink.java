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

package com.riskfocus.flink.sink.jdbc;

import com.riskfocus.flink.sink.jdbc.config.JdbcConnectionOptions;
import com.riskfocus.flink.sink.jdbc.config.JdbcExecutionOptions;
import com.riskfocus.flink.sink.jdbc.connector.SimpleJdbcConnectionProvider;
import com.riskfocus.flink.sink.jdbc.core.executor.JdbcBatchStatementExecutor;
import com.riskfocus.flink.sink.jdbc.core.output.JdbcBatchingOutputFormat;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.io.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Preconditions;

import java.util.function.Function;

/**
 * @author Khokhlov Pavel
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class JdbcSink {

    public static <T> SinkFunction<T> sink(
            String sql,
            JdbcStatementBuilder<T> statementBuilder,
            JdbcExecutionOptions executionOptions,
            JdbcConnectionOptions connectionOptions) {
        return new JdbcSinkFunction<>(new JdbcBatchingOutputFormat<>(
                new SimpleJdbcConnectionProvider(connectionOptions),
                executionOptions,
                context -> {
                    Preconditions.checkState(!context.getExecutionConfig().isObjectReuseEnabled(),
                            "objects can not be reused with JDBC sink function");
                    return JdbcBatchStatementExecutor.simple(sql, statementBuilder, Function.identity(), executionOptions);
                },
                JdbcBatchingOutputFormat.RecordExtractor.identity()
        ));
    }
}
