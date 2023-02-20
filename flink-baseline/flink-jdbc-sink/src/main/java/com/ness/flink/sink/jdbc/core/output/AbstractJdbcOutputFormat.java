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

package com.ness.flink.sink.jdbc.core.output;

import static org.apache.flink.util.Preconditions.checkNotNull;

import com.ness.flink.sink.jdbc.config.JdbcExecutionOptions;
import com.ness.flink.sink.jdbc.connector.JdbcConnectionProvider;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.api.connector.sink2.SinkWriter;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
public abstract class AbstractJdbcOutputFormat<T> implements SinkWriter<T>, Serializable {
    private static final long serialVersionUID = 1L;

    static final String SQL_STATE_CONNECTION_CLOSED = "SQL:connection:closed";
    static final String SQL_STATE_CONNECTION_IS_NOT_VALID = "SQL:connection:isNotValid";

    protected final String sinkName;
    private final JdbcConnectionProvider connectionProvider;
    protected transient InitContext context;
    protected transient Connection connection;
    private transient AtomicLong lastTimeConnectionUsage;

    protected final JdbcExecutionOptions executionOptions;

    AbstractJdbcOutputFormat(String sinkName, JdbcExecutionOptions executionOptions, JdbcConnectionProvider connectionProvider) {
        this.sinkName = checkNotNull(sinkName);
        this.executionOptions = checkNotNull(executionOptions);
        this.connectionProvider = checkNotNull(connectionProvider);
    }

    @SneakyThrows
    public void open(InitContext context) throws IOException {
        this.context = context;
        connection = getConnection();
    }

    /**
     * Reinitialization
     *
     * @param connection new jdbc connection
     * @throws SQLException
     */
    abstract void reinit(Connection connection) throws SQLException;

    /**
     * Release current connection related resources
     */
    abstract void closeStatement();

    protected Connection getConnection() throws SQLException, IOException {
        Connection newConnection;
        try {
            newConnection = connectionProvider.establishConnection();
            lastTimeConnectionUsage = new AtomicLong(now());
            return newConnection;
        } catch (ClassNotFoundException e) {
            throw new IOException("Can't open connection", e);
        }
    }

    void recover(int maxRetries, SQLException e) throws IOException {
        log.info("Unsuccessful SQL execution. maxRetries={}, error details={}, type={}", maxRetries, e.getMessage(),
            e.getSQLState());
        rollback(e);
        retryConnection(maxRetries, e);
    }

    void retryConnection(int maxRetries, SQLException e) throws IOException {
        int retryCnt = 1;
        while (retryCnt <= maxRetries) {
            try {
                closeStatement();
                closeConnection();
                connection = getConnection();
                reinit(connection);
                return;
            } catch (SQLException sqlException) {
                log.warn("Got issue with connection again, retry={}, error={}", retryCnt, sqlException.getMessage());
                retryCnt = retryCnt + 1;
                sleep(retryCnt);
            }
        }
        throw new IOException("Max retry count exceeded. Unable to establish new connection", e);
    }

    void sleep(int retryCnt) throws IOException {
        try {
            Thread.sleep(retryCnt * 1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Unable to flush", e);
        }
    }

    private void rollback(SQLException cause) {
        if (cause != null) {
            String sqlState = cause.getSQLState();
            if (sqlState != null) {
                switch (sqlState) {
                    case SQL_STATE_CONNECTION_CLOSED:
                    case SQL_STATE_CONNECTION_IS_NOT_VALID:
                        return;
                }
            }
        }
        if (connection != null) {
            try {
                if (!connection.isClosed()) {
                    connection.rollback();
                }
            } catch(SQLException e) {
                log.warn("Transaction rollback failed: {}", e.getMessage());
            }
        }
    }

    void updateLastTimeConnectionUsage() {
        lastTimeConnectionUsage.set(now());
    }

    boolean checkConnectionRequired() {
        long elapsedTime = now() - lastTimeConnectionUsage.get();
        boolean checkRequired = elapsedTime > executionOptions.getConnectionCheckMaxIdleMs();
        if (checkRequired) {
            log.debug("Passed: {} ms, connection checking required", elapsedTime);
        }
        return checkRequired;
    }

    void checkConnection() throws SQLException {
        if (checkConnectionRequired()) {
            if (connection.isClosed()) {
                throw new SQLException("Connection is closed", SQL_STATE_CONNECTION_CLOSED);
            }
            if (!connectionProvider.isConnectionValid(connection)) {
                throw new SQLException("Connection is not valid", SQL_STATE_CONNECTION_IS_NOT_VALID);
            }
        }
    }

    long now() {
        return System.currentTimeMillis();
    }

    void closeConnection() {
        try {
            connectionProvider.closeConnection(connection);
        } finally {
            connection = null;
        }
    }

    @Override
    public void close() {
        closeConnection();
    }

}
