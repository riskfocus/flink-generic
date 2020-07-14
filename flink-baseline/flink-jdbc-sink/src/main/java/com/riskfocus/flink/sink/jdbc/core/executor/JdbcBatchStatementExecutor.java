package com.riskfocus.flink.sink.jdbc.core.executor;

import com.riskfocus.flink.sink.jdbc.config.JdbcExecutionOptions;
import com.riskfocus.flink.sink.jdbc.core.JdbcSqlBuilderWithMetadata;
import com.riskfocus.flink.sink.jdbc.core.JdbcStatementBuilderWithMetadata;
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

    /**
     * Batch statement Executor with table metadata reading
     * @param tableName table name
     * @param sql function accepts table metadata and returns SQL statement
     * @param statementBuilder functions accepts table metadata and returns consumer function to be applied on a value
     * @param valueTransformer
     * @param jdbcExecutionOptions
     * @param <T>
     * @param <V>
     * @return
     */
    static <T, V> JdbcBatchStatementExecutor<T> withTableMetadata(
            String tableName,
            JdbcSqlBuilderWithMetadata sql,
            JdbcStatementBuilderWithMetadata<V> statementBuilder,
            Function<T, V> valueTransformer, JdbcExecutionOptions jdbcExecutionOptions
    ) {
        return new WithTableMetadataBatchStatementExecutor<>(tableName, sql, statementBuilder, valueTransformer, jdbcExecutionOptions);
    }
}