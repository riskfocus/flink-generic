package com.riskfocus.flink.snapshot;

import com.riskfocus.flink.snapshot.context.ContextMetadata;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
public abstract class SnapshotMapper<T> implements Serializable {
    private static final long serialVersionUID = -4643297908380913314L;

    private static final String snapShot = "snapshot";
    private static final String index = "index";

    protected final String delimiter;

    /**
     * Provides prefix for snapshot
     * @param ctx metadata context
     * @return
     */
    public String buildSnapshotPrefix(ContextMetadata ctx) {
        return snapShot + delimiter + ctx.getContextName();
    }

    /**
     * Build index name for that specific data
     * Some storage types might support indexes as well (e.g: Redis)
     * @param ctx metadata context
     * @return name of index
     */
    public String buildSnapshotIndexKey(ContextMetadata ctx) {
        return buildSnapshotPrefix(ctx) + delimiter + index;
    }

    /**
     * Provides KEY for that specific data in storage
     *
     * @param data data which will be put into storage
     * @param ctx metadata context
     * @return key which will be held that data in storage
     */
    public abstract String buildKey(T data, ContextMetadata ctx);

    /**
     * Converts specific data to String representation
     * @param data event
     * @return string presentation of data
     * @throws IOException
     */
    public abstract String getValueFromData(T data) throws IOException;
}
