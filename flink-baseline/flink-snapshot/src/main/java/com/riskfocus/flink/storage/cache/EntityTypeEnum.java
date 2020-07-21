/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.storage.cache;

import com.google.common.annotations.Beta;

@Beta
public enum EntityTypeEnum {

    MEM_CACHE_ONLY(":"),
    STORAGE_ONLY("/"),
    MEM_CACHE_BACKED_BY_STORAGE("/"),
    MEM_CACHE_WITH_INDEX_SUPPORT_ONLY(":");

    private String delimiter;

    EntityTypeEnum(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getDelimiter() {
        return delimiter;
    }
}
