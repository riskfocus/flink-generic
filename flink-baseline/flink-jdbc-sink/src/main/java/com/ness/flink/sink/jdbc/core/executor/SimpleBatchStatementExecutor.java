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
import org.apache.flink.api.java.io.jdbc.JdbcStatementBuilder;

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

    private transient PreparedStatement st;
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
        this.st = connection.prepareStatement(sql);
    }

    @Override
    public void addToBatch(T record) {
        batch.add(valueTransformer.apply(record));
    }

    @Override
    public void executeBatch() throws SQLException {
        if (!batch.isEmpty()) {
            for (V r : batch) {
                parameterSetter.accept(st, r);
                st.addBatch();
                log.trace("Added to batch: {}", r);
            }
            st.executeBatch();
            if (!connection.getAutoCommit()) {
                log.trace("Commit batch: {}", batch.size());
                connection.commit();
            }
            batch.clear();
        }
    }

    @Override
    public void close() throws SQLException {
        if (st != null) {
            st.close();
            st = null;
        }
        if (batch != null) {
            batch.clear();
        }
    }
}
