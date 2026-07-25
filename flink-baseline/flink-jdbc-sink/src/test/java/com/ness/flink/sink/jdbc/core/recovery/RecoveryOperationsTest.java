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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.SQLException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RecoveryOperationsTest {

    private RecoveryOperations build(JdbcConnectionProvider provider, JdbcStatementExecutor executor,
                                     JdbcExecutionOptions options) throws Exception {
        return new RecoveryOperations(provider, options, executor);
    }

    @Test
    void shouldInitStatementExecutorOnConstruction() throws Exception {
        Connection connection = mock(Connection.class);
        JdbcConnectionProvider provider = mock(JdbcConnectionProvider.class);
        when(provider.getConnection()).thenReturn(connection);
        JdbcStatementExecutor executor = mock(JdbcStatementExecutor.class);

        build(provider, executor, JdbcExecutionOptions.builder().build());

        verify(provider).getConnection();
        verify(executor).init(connection);
    }

    @Test
    void shouldThrowConnectionClosedStateWhenConnectionClosedAndCheckDue() throws Exception {
        Connection connection = mock(Connection.class);
        when(connection.isClosed()).thenReturn(true);
        JdbcConnectionProvider provider = mock(JdbcConnectionProvider.class);
        when(provider.getConnection()).thenReturn(connection);

        // negative idle threshold forces the connection check to run
        RecoveryOperations recovery = build(provider, mock(JdbcStatementExecutor.class),
            JdbcExecutionOptions.builder().withConnectionCheckMaxIdleMs(-1).build());

        SQLException ex = Assertions.assertThrows(SQLException.class, recovery::checkConnection);
        Assertions.assertEquals("SQL:connection:closed", ex.getSQLState());
    }

    @Test
    void shouldRollbackAndThrowOnNonRetryableErrorWhenNotIgnoring() throws Exception {
        Connection connection = mock(Connection.class);
        when(connection.isClosed()).thenReturn(false);
        JdbcConnectionProvider provider = mock(JdbcConnectionProvider.class);
        when(provider.getConnection()).thenReturn(connection);

        RecoveryOperations recovery = build(provider, mock(JdbcStatementExecutor.class),
            JdbcExecutionOptions.builder().withIgnoreSQLExceptions(false).build());

        SQLException nonRetryable = new SQLException("constraint violation", "23000");
        Assertions.assertThrows(FailedSQLExecution.class, () -> recovery.recover(nonRetryable, 1));
        verify(connection).rollback();
    }

    @Test
    void shouldSwallowNonRetryableErrorWhenIgnoring() throws Exception {
        Connection connection = mock(Connection.class);
        when(connection.isClosed()).thenReturn(false);
        JdbcConnectionProvider provider = mock(JdbcConnectionProvider.class);
        when(provider.getConnection()).thenReturn(connection);

        RecoveryOperations recovery = build(provider, mock(JdbcStatementExecutor.class),
            JdbcExecutionOptions.builder().withIgnoreSQLExceptions(true).build());

        SQLException nonRetryable = new SQLException("constraint violation", "23000");
        Assertions.assertDoesNotThrow(() -> recovery.recover(nonRetryable, 1));
    }

    @Test
    void shouldCloseUnderlyingConnection() throws Exception {
        Connection connection = mock(Connection.class);
        JdbcConnectionProvider provider = mock(JdbcConnectionProvider.class);
        when(provider.getConnection()).thenReturn(connection);

        RecoveryOperations recovery = build(provider, mock(JdbcStatementExecutor.class),
            JdbcExecutionOptions.builder().build());
        recovery.closeConnection();

        verify(connection).close();
    }
}
