/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.sink.jdbc.core.executor;

import com.riskfocus.flink.sink.jdbc.config.JdbcExecutionOptions;
import com.riskfocus.flink.sink.jdbc.core.FieldMetadata;
import com.riskfocus.flink.sink.jdbc.core.JdbcSqlBuilderWithMetadata;
import com.riskfocus.flink.sink.jdbc.core.JdbcStatementBuilderWithMetadata;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

/**
 * Another implementation of {@link SimpleBatchStatementExecutor} with table metadata reading once connection is open
 * @author NIakovlev
 */
@Slf4j
public class WithTableMetadataBatchStatementExecutor<T, V> extends AbstractSimpleBatchStatementExecutor<T, V> {

    private final String tableName;
    private final JdbcSqlBuilderWithMetadata sqlBuilderWithMetadata;
    private final JdbcStatementBuilderWithMetadata<V> statementBuilder;

    private transient List<FieldMetadata> tableMetadata;

    public WithTableMetadataBatchStatementExecutor(String tableName,
                                                   JdbcSqlBuilderWithMetadata sqlBuilderWithMetadata,
                                                   JdbcStatementBuilderWithMetadata<V> statementBuilder,
                                                   Function<T, V> valueTransformer,
                                                   JdbcExecutionOptions jdbcExecutionOptions) {
        super(valueTransformer, jdbcExecutionOptions);
        this.tableName = tableName;
        this.sqlBuilderWithMetadata = sqlBuilderWithMetadata;
        this.statementBuilder = statementBuilder;
    }

    @Override
    void init(Connection connection) throws SQLException {
        this.tableMetadata = getTableMetadata();
    }

    @Override
    JdbcStatementBuilderWithMetadata<V> getStatementConsumer() {
        return statementBuilder;
    }

    @Override
    String getSQL() {
        return sqlBuilderWithMetadata.apply(tableMetadata);
    }

    @Override
    Collection<FieldMetadata> getMetadata() {
        return tableMetadata;
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

}
