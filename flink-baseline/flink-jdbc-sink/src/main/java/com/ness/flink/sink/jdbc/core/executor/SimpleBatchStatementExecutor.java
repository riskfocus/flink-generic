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
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
public class SimpleBatchStatementExecutor<T, V> implements JdbcBatchStatementExecutor<T> {

    private final String sql;
    private final JdbcStatementBuilder<V> parameterSetter;
    private final Function<T, V> valueTransformer;
    private final transient List<V> batch;

    private transient PreparedStatement preparedStatement;
    private transient Connection connection;

    public SimpleBatchStatementExecutor(String sql, JdbcStatementBuilder<V> statementBuilder, Function<T, V> valueTransformer,
                                        JdbcExecutionOptions jdbcExecutionOptions) {
        this.sql = sql;
        this.parameterSetter = statementBuilder;
        this.valueTransformer = valueTransformer;
        this.batch = new ArrayList<>(jdbcExecutionOptions.getBatchSize());
    }

    @Override
    public void open(Connection connection) throws SQLException {
        this.connection = connection;
        this.preparedStatement = connection.prepareStatement(sql);
    }

    @Override
    public void reinit(Connection connection) throws SQLException {
        closeStatement();
        open(connection);
    }

    @Override
    public void addToBatch(T message) {
        batch.add(valueTransformer.apply(message));
    }

    @Override
    @SuppressWarnings("PMD.GuardLogStatement")
    public void executeBatch() throws SQLException {
        if (!batch.isEmpty()) {
            for (V r : batch) {
                parameterSetter.accept(preparedStatement, r);
                preparedStatement.addBatch();
                log.trace("Added to batch: {}", r);
            }
            preparedStatement.executeBatch();
            if (!connection.getAutoCommit()) {
                log.debug("Commit batch: {}", batch.size());
                connection.commit();
            }
            batch.clear();
        }
    }

    @Override
    public void closeStatement() {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                log.warn("Cannot close statement");
            }
        }
    }

    @Override
    public void close() {
        closeStatement();
        if (batch != null) {
            batch.clear();
        }
    }
}
