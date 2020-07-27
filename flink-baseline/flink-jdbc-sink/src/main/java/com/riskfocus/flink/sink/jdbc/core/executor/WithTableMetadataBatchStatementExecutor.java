/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.sink.jdbc.core.executor;

import com.riskfocus.flink.sink.jdbc.config.JdbcExecutionOptions;
import com.riskfocus.flink.sink.jdbc.core.FieldMetadata;
import com.riskfocus.flink.sink.jdbc.core.JdbcSqlBuilderWithMetadata;
import com.riskfocus.flink.sink.jdbc.core.JdbcStatementBuilderWithMetadata;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Another implementation of {@link SimpleBatchStatementExecutor} with table metadata reading once connection is open
 * @author NIakovlev
 */
@Slf4j
public class WithTableMetadataBatchStatementExecutor<T, V> implements JdbcBatchStatementExecutor<T> {

    private final String tableName;
    private final JdbcSqlBuilderWithMetadata sql;
    private final JdbcStatementBuilderWithMetadata<V> statementBuilder;
    private final Function<T, V> valueTransformer;
    private final transient List<V> batch;

    private transient PreparedStatement st;
    private transient Connection connection;
    private transient List<FieldMetadata> tableMetadata;

    public WithTableMetadataBatchStatementExecutor(String tableName,
                                                   JdbcSqlBuilderWithMetadata sql,
                                                   JdbcStatementBuilderWithMetadata<V> statementBuilder,
                                                   Function<T, V> valueTransformer,
                                                   JdbcExecutionOptions jdbcExecutionOptions) {
        this.tableName = tableName;
        this.sql = sql;
        this.statementBuilder = statementBuilder;
        this.valueTransformer = valueTransformer;
        this.batch = new ArrayList<>(jdbcExecutionOptions.getBatchSize());
    }

    @Override
    public void open(Connection connection) throws SQLException {
        this.connection = connection;
        this.tableMetadata = getTableMetadata();
        this.st = connection.prepareStatement(sql.apply(tableMetadata));
    }

    private List<FieldMetadata> getTableMetadata() throws SQLException {
        ResultSet descSet = connection.prepareStatement("DESCRIBE " + tableName).executeQuery();
        ResultSet columnsSet = connection.prepareStatement("select * from " + tableName + " limit 0").executeQuery();

        ResultSetMetaData metaData = columnsSet.getMetaData();
        List<FieldMetadata> fieldMetadataList = new ArrayList<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            descSet.next(); //move the cursor

            fieldMetadataList.add(FieldMetadata.builder()
                    .fieldName(metaData.getColumnName(i))
                    .jdbcType(JDBCType.valueOf(metaData.getColumnType(i)))
                    .nullable("YES".equals(descSet.getString(3)))
                    .primaryKey("PRI".equals(descSet.getString(4)))
                    .withDefault(descSet.getObject(5) != null)
                    .build()
            );
        }
        connection.commit(); //close transaction for releasing metadata lock
        return fieldMetadataList;
    }

    @Override
    public void addToBatch(T record) {
        batch.add(valueTransformer.apply(record));
    }

    @Override
    public void executeBatch() throws SQLException {
        if (!batch.isEmpty()) {
            for (V r : batch) {
                statementBuilder.accept(st, tableMetadata, r);
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
