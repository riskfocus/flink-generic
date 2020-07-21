/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @author Khokhlov Pavel
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DateTimeUtils {
    private static final ZoneId UTC = ZoneId.of("UTC");
    private static final DateTimeFormatter DATE_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyyMMdd");

    public static String format(long timestamp) {
        return format(timestamp, DATE_TIME);
    }

    public static String format(long timestamp, DateTimeFormatter pattern) {
        return Instant.ofEpochMilli(timestamp).atZone(UTC).format(pattern);
    }

    public static String formatDate(long timestamp) {
        return Instant.ofEpochMilli(timestamp).atZone(UTC).format(DATE);
    }
}
