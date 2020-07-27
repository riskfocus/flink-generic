/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.sink.jdbc.core.executor;

import com.riskfocus.flink.sink.jdbc.config.JdbcExecutionOptions;
import com.riskfocus.flink.sink.jdbc.core.JdbcStatementBuilderWithMetadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.io.jdbc.JdbcStatementBuilder;

import java.util.function.Function;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
public class SimpleBatchStatementExecutor<T, V> extends AbstractSimpleBatchStatementExecutor<T, V> {

    private final String sql;
    private final JdbcStatementBuilder<V> parameterSetter;

    public SimpleBatchStatementExecutor(String sql, JdbcStatementBuilder<V> statementBuilder, Function<T, V> valueTransformer,
                                        JdbcExecutionOptions jdbcExecutionOptions) {
        super(valueTransformer, jdbcExecutionOptions);
        this.sql = sql;
        this.parameterSetter = statementBuilder;
    }

    @Override
    String getSQL() {
        return sql;
    }

    @Override
    JdbcStatementBuilderWithMetadata<V> getStatementConsumer() {
        return (preparedStatement, fieldMetadata, v) -> parameterSetter.accept(preparedStatement, v);
    }
}
