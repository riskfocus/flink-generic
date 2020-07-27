/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.sink.jdbc.core;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;

/**
 * Consumer function with 3 arguments:
 * <ul>
 *     <li>{@link PreparedStatement} - statement to be filled in with new values</li>
 *     <li>Collection<{@link FieldMetadata}> - table metadata with list of column info</li>
 *     <li>T - new record to be processed by the Sink function</li>
 * </ul>
 * @param <T>
 */
@PublicEvolving
public interface JdbcStatementBuilderWithMetadata<T> extends TriConsumerWithException<PreparedStatement, Collection<FieldMetadata>, T, SQLException>, Serializable {
}
