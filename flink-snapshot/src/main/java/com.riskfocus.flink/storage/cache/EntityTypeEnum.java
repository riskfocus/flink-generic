package com.riskfocus.flink.storage.cache;

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
