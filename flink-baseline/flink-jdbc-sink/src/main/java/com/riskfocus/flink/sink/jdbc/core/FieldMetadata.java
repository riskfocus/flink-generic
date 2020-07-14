package com.riskfocus.flink.sink.jdbc.core;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.sql.JDBCType;

/**
 * Class contains information about DB field
 */
@AllArgsConstructor
@Builder
@Getter
public class FieldMetadata {

    private final String fieldName;
    private final JDBCType jdbcType;
    /**
     * is field a part of PRIMARY KEY
     */
    private final boolean isPrimary;
    /**
     * can field be a NULL
     */
    private final boolean nullable;
    /**
     * Does field have a default value
     */
    private final boolean withDefault;
}
