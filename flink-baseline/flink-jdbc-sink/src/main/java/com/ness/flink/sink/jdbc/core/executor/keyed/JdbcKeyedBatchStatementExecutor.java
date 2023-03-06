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

package com.ness.flink.sink.jdbc.core.executor.keyed;

import com.ness.flink.sink.jdbc.config.JdbcExecutionOptions;
import com.ness.flink.sink.jdbc.core.executor.JdbcStatementBuilder;
import com.ness.flink.sink.jdbc.core.executor.JdbcStatementExecutor;
import com.ness.flink.sink.jdbc.core.output.FailedSQLExecution;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
@Internal
@RequiredArgsConstructor
public class JdbcKeyedBatchStatementExecutor<I> implements JdbcStatementExecutor {

    private final String sql;
    private final JdbcStatementBuilder<I> jdbcStatementBuilder;
    private final JdbcExecutionOptions jdbcExecutionOptions;

    private final ListState<I> state;
    private final ValueState<Integer> count;

    private PreparedStatement preparedStatement;
    private Connection connection;

    /**
     * Register item for batch
     *
     * @param value data for batching
     * @return number of records in current batch
     * @throws Exception in case of issue with Flink state
     */
    public int addToBatch(I value) throws Exception {
        // Register data for batch
        state.add(value);
        int currentCount = getCurrentBatchSize();
        currentCount++;
        count.update(currentCount);
        return currentCount;
    }

    /**
     * Executes batch
     *
     * @return records in batch
     * @throws SQLException in case of SQL error
     */
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    public List<I> executeBatch() throws SQLException {
        Iterable<I> iterable;
        try {
            iterable = state.get();
        } catch (Exception e) {
            throw new FailedSQLExecution(e);
        }
        List<I> emissionData = new ArrayList<>();
        for (I item : iterable) {
            jdbcStatementBuilder.accept(preparedStatement, item);
            preparedStatement.addBatch();
            log.trace("Added to batch: {}", item);
            emissionData.add(item);
        }
        if (emissionData.isEmpty()) {
           return emissionData;
        } else {
            preparedStatement.executeBatch();
            if (!connection.getAutoCommit()) {
                if (log.isDebugEnabled()) {
                    log.debug("Commit batch: {}", emissionData.size());
                }
                connection.commit();
            }
        }
        return emissionData;
    }

    /**
     * Clean-up current state
     * @throws IOException in case of error with Flink state related operations
     */
    public void cleanUp() throws IOException {
        state.clear();
        count.update(0);
    }

    /**
     * Checks if batch is full of data
     * @return if batch is full of data
     * @throws IOException in case of error with Flink state related operations
     */
    public boolean batchCompleted() throws IOException {
        int batchSize = jdbcExecutionOptions.getBatchSize();
        Integer stateSize = count.value();
        return stateSize != null && stateSize >= batchSize;
    }

    /**
     * Number of records in current batch
     * @return number of records in current batch
     * @throws IOException in case of error with Flink state related operations
     */
    public int getCurrentBatchSize() throws IOException {
        return Optional.ofNullable(count.value()).orElse(0);
    }

    @Override
    public void init(Connection connection) throws SQLException {
        this.connection = connection;
        this.preparedStatement = connection.prepareStatement(sql);
    }

    @Override
    public void reInit(Connection connection) throws SQLException {
        closeStatement();
        init(connection);
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
    }
}
