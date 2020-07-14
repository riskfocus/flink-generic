package com.riskfocus.flink.sink.jdbc;

import com.riskfocus.flink.sink.jdbc.config.JdbcConnectionOptions;
import com.riskfocus.flink.sink.jdbc.config.JdbcExecutionOptions;
import com.riskfocus.flink.sink.jdbc.connector.SimpleJdbcConnectionProvider;
import com.riskfocus.flink.sink.jdbc.core.JdbcSqlBuilderWithMetadata;
import com.riskfocus.flink.sink.jdbc.core.JdbcStatementBuilderWithMetadata;
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

    public static <T> SinkFunction<T> sinkWithTableMetadata(
            String tableName,
            JdbcSqlBuilderWithMetadata sql,
            JdbcStatementBuilderWithMetadata<T> statementBuilder,
            JdbcExecutionOptions executionOptions,
            JdbcConnectionOptions connectionOptions) {
        return new JdbcSinkFunction<>(new JdbcBatchingOutputFormat<>(
                new SimpleJdbcConnectionProvider(connectionOptions),
                executionOptions,
                context -> {
                    Preconditions.checkState(!context.getExecutionConfig().isObjectReuseEnabled(),
                            "objects can not be reused with JDBC sink function");
                    return JdbcBatchStatementExecutor.withTableMetadata(tableName, sql, statementBuilder, Function.identity(), executionOptions);
                },
                JdbcBatchingOutputFormat.RecordExtractor.identity()
        ));
    }
}
