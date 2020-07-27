/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.sink.jdbc.core.executor;

import com.riskfocus.flink.sink.jdbc.config.JdbcExecutionOptions;
import com.riskfocus.flink.sink.jdbc.core.FieldMetadata;
import com.riskfocus.flink.sink.jdbc.core.JdbcStatementBuilderWithMetadata;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
public abstract class AbstractSimpleBatchStatementExecutor<T, V> implements JdbcBatchStatementExecutor<T> {

    private final Function<T, V> valueTransformer;
    private final transient List<V> batch;
    private transient PreparedStatement st;
    protected transient Connection connection;

    public AbstractSimpleBatchStatementExecutor(Function<T, V> valueTransformer,
                                        JdbcExecutionOptions jdbcExecutionOptions) {
        this.valueTransformer = valueTransformer;
        this.batch = new ArrayList<>(jdbcExecutionOptions.getBatchSize());
    }

    @Override
    public final void open(Connection connection) throws SQLException {
        this.connection = connection;
        init(connection);
        this.st = connection.prepareStatement(getSQL());
    }

    Collection<FieldMetadata> getMetadata() {
        return Collections.emptyList();
    }

    void init(Connection connection) throws SQLException {

    }

    abstract String getSQL();
    abstract JdbcStatementBuilderWithMetadata<V> getStatementConsumer();

    @Override
    public final void addToBatch(T record) {
        batch.add(valueTransformer.apply(record));
    }

    @Override
    public final void executeBatch() throws SQLException {
        if (!batch.isEmpty()) {
            for (V r : batch) {
                getStatementConsumer().accept(st, getMetadata(), r);
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
    public final void close() throws SQLException {
        if (st != null) {
            st.close();
            st = null;
        }
        if (batch != null) {
            batch.clear();
        }
    }

}
