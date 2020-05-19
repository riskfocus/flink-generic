package com.riskfocus.flink.sink.jdbc.core.executor;

import com.riskfocus.flink.sink.jdbc.config.JdbcExecutionOptions;
import org.apache.flink.api.java.io.jdbc.JdbcStatementBuilder;

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

    void addToBatch(T record) throws SQLException;

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