package com.riskfocus.flink.snapshot;

import com.google.common.annotations.Beta;
import com.riskfocus.flink.batch.BatchAware;
import com.riskfocus.flink.util.DateTimeUtils;
import lombok.AllArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author Khokhlov Pavel
 */
@Beta
@AllArgsConstructor
public abstract class SnapshotMapper<T> implements Serializable {
    private static final long serialVersionUID = -4643297908380913314L;

    protected static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String snapShot = "snapshot";
    private static final String index = "index";

    protected final BatchAware batchAware;
    protected final String delimiter;

    protected String buildPrefix() {
        return snapShot + delimiter + getEntityPrefix();
    }

    public byte[] buildIndexKey() {
        return (buildPrefix() + delimiter + index).getBytes();
    }

    public byte[] buildPrefixBin() {
        return buildPrefix().getBytes();
    }

    protected abstract String getEntityPrefix();

    public abstract byte[] buildKey(T data, long window);

    public abstract byte[] getValueFromData(T data) throws IOException;

    public abstract long getWindowId(T data);

    public String convert(long windowId) {
        long timestamp = batchAware.convertToTimestamp(windowId);
        return DateTimeUtils.formatDate(timestamp);
    }

}
