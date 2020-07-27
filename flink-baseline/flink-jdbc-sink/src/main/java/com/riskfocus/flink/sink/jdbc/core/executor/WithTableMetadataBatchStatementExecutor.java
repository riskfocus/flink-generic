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
        List<FieldMetadata> fieldMetadataList = new ArrayList<>();
        List<String> primaryKeys = getPrimaryKeysDesc();

        try (ResultSet columns = connection.getMetaData().getColumns(null, null, tableName, null)) {
            while (columns.next()) {
                // See java.sql.DatabaseMetaData.getColumns documentation
                String columnName = columns.getString("COLUMN_NAME");
                JDBCType dataType = JDBCType.valueOf(columns.getInt("DATA_TYPE"));
                String nullabe = columns.getString("IS_NULLABLE");
                String defaultValue = columns.getString("COLUMN_DEF");
                boolean primaryKey = primaryKeys.contains(columnName);
                log.info("Got description for table: {}, columnName: {}, dataType: {}, nullabe: {}, defaultValue: {}, primaryKey: {}", tableName, columnName,
                        dataType, nullabe, defaultValue,
                        primaryKey);
                fieldMetadataList.add(FieldMetadata.builder()
                        .fieldName(columnName)
                        .jdbcType(dataType)
                        .nullable("YES".equalsIgnoreCase(nullabe))
                        .primaryKey(primaryKey)
                        .withDefault(defaultValue != null)
                        .build());
            }
        }
        return fieldMetadataList;
    }

    private List<String> getPrimaryKeysDesc() throws SQLException {
        List<String> res = new ArrayList<>();
        try (ResultSet primaryKeys = connection.getMetaData().getPrimaryKeys(null, null, tableName)) {
            while (primaryKeys.next()) {
                String column = primaryKeys.getString("COLUMN_NAME");
                res.add(column);
            }
        }
        return res;
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
