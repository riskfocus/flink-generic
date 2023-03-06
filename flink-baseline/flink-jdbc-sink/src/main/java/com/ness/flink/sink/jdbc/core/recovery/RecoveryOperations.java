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

package com.ness.flink.sink.jdbc.core.recovery;

import com.ness.flink.sink.jdbc.config.JdbcExecutionOptions;
import com.ness.flink.sink.jdbc.connector.JdbcConnectionProvider;
import com.ness.flink.sink.jdbc.core.executor.JdbcStatementExecutor;
import com.ness.flink.sink.jdbc.core.output.FailedSQLExecution;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
@SuppressWarnings("PMD.TooManyMethods")
public class RecoveryOperations {
    private static final String SQL_STATE_CONNECTION_CLOSED = "SQL:connection:closed";
    private static final String SQL_STATE_CONNECTION_IS_NOT_VALID = "SQL:connection:isNotValid";

    private final JdbcConnectionProvider jdbcConnectionProvider;
    private final JdbcExecutionOptions jdbcExecutionOptions;
    private final JdbcStatementExecutor jdbcStatementExecutor;


    private AtomicLong lastTimeConnectionUsage;
    private volatile Connection connection;

    public RecoveryOperations(JdbcConnectionProvider jdbcConnectionProvider, JdbcExecutionOptions jdbcExecutionOptions,
        JdbcStatementExecutor jdbcStatementExecutor) throws SQLException, IOException {
        this.jdbcConnectionProvider = jdbcConnectionProvider;
        this.jdbcExecutionOptions = jdbcExecutionOptions;
        this.jdbcStatementExecutor = jdbcStatementExecutor;
        this.connection = establishConnection();
        this.jdbcStatementExecutor.init(connection);
    }

    public void recover(SQLException sqlException, int attempt) throws IOException {
        int maxRetries = jdbcExecutionOptions.getMaxRetries();
        String sqlState = sqlException.getSQLState();
        if (log.isInfoEnabled()) {
            log.info("Unsuccessful SQL execution. attempt={}, maxRetries={}, error details={}, type={}",
                attempt, maxRetries, sqlException.getMessage(), sqlState);
        }
        rollback(sqlException);
        if (SQL_STATE_CONNECTION_CLOSED.equals(sqlState) || SQL_STATE_CONNECTION_IS_NOT_VALID.equals(sqlState)) {
            // We do retry only in case of closed connection
            retryConnection(maxRetries, sqlException);
        } else {
            if (jdbcExecutionOptions.isIgnoreSQLExceptions()) {
                log.warn("Got non-retryable error. Possible potential data loss", sqlException);
                return;
            }
            throw new FailedSQLExecution(sqlException);
        }
    }

    public void checkConnection() throws SQLException {
        if (checkConnectionRequired()) {
            if (connection.isClosed()) {
                throw new SQLException("Connection is closed", SQL_STATE_CONNECTION_CLOSED);
            }
            if (!isConnectionValid()) {
                throw new SQLException("Connection is not valid", SQL_STATE_CONNECTION_IS_NOT_VALID);
            }
        }
    }

    public void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.warn("JDBC connection close failed.", e);
            }
        }
    }

    public void updateLastTimeConnectionUsage() {
        lastTimeConnectionUsage.set(now());
    }

    private void rollback(SQLException cause) {
        if (cause != null) {
            String sqlState = cause.getSQLState();
            if (SQL_STATE_CONNECTION_CLOSED.equals(sqlState) || SQL_STATE_CONNECTION_IS_NOT_VALID.equals(sqlState)) {
                return;
            }
        }
        if (connection != null) {
            try {
                if (!connection.isClosed()) {
                    connection.rollback();
                }
            } catch (SQLException e) {
                log.warn("Transaction rollback failed:", e);
            }
        }
    }

    private boolean checkConnectionRequired() {
        long elapsedTime = now() - lastTimeConnectionUsage.get();
        boolean checkRequired = elapsedTime > jdbcExecutionOptions.getConnectionCheckMaxIdleMs();
        if (checkRequired) {
            log.debug("Passed: {} ms, connection checking required", elapsedTime);
        }
        return checkRequired;
    }

    private Connection establishConnection() throws SQLException, IOException {
        try {
            Connection newConnection = jdbcConnectionProvider.getConnection();
            lastTimeConnectionUsage = new AtomicLong(now());
            return newConnection;
        } catch (ClassNotFoundException e) {
            throw new IOException("Can't open connection", e);
        }
    }

    private long now() {
        return System.currentTimeMillis();
    }

    private boolean isConnectionValid() throws SQLException {
        return connection != null && connection.isValid(jdbcExecutionOptions.getConnectionCheckTimeoutSeconds());
    }

    protected void reInit(Connection connection) throws SQLException {
        jdbcStatementExecutor.reInit(connection);
    }

    private void retryConnection(int maxRetries, SQLException rootSqlException) throws IOException {
        int retryCnt = 1;
        while (retryCnt <= maxRetries) {
            try {
                jdbcStatementExecutor.closeStatement();
                closeConnection();
                connection = establishConnection();
                reInit(connection);
                return;
            } catch (SQLException sqlException) {
                if (log.isWarnEnabled()) {
                    log.warn("Got issue with connection again, retry={}, error={}", retryCnt,
                        sqlException.getMessage());
                }
                retryCnt = retryCnt + 1;
                sleep(retryCnt);
            }
        }
        throw new IOException("Max retry count exceeded. Unable to establish new connection", rootSqlException);
    }

    private void sleep(int retryCnt) throws IOException {
        try {
            Thread.sleep(retryCnt * jdbcExecutionOptions.getWaitBeforeRetryMs());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Unable to flush", e);
        }
    }

}
