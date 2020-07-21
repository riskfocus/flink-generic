/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.sink.jdbc.core;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.List;
import java.util.function.Function;

/**
 * Function with an argument of List<{@link FieldMetadata}> read from the DB. Return argument - an SQL query to be executed
 */
@PublicEvolving
public interface JdbcSqlBuilderWithMetadata extends Function<List<FieldMetadata>, String>, Serializable {
}
