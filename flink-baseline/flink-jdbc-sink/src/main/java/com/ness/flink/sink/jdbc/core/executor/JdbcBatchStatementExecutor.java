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

package com.ness.flink.sink.jdbc.core.executor;

import com.ness.flink.sink.jdbc.config.JdbcExecutionOptions;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Function;

/**
 * @author Khokhlov Pavel
 */
public interface JdbcBatchStatementExecutor<T> {

    /**
     * Open the writer by JDBC Connection. It can create Statement from Connection.
     */
    void open(Connection connection) throws SQLException;

    void addToBatch(T message) throws SQLException;

    /**
     * Submits a batch of commands to the database for execution.
     */
    void executeBatch() throws SQLException;

    /**
     * Close JDBC related statements and other classes.
     */
    void close() throws SQLException;

    static <T, V> JdbcBatchStatementExecutor<T> simple(String sql, JdbcStatementBuilder<V> paramSetter,
                                                       Function<T, V> valueTransformer, JdbcExecutionOptions jdbcExecutionOptions) {
        return new SimpleBatchStatementExecutor<>(sql, paramSetter, valueTransformer, jdbcExecutionOptions);
    }

}