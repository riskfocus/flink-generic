package com.riskfocus.flink.snapshot;

import com.riskfocus.flink.snapshot.context.Context;
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

    public String buildSnapshotPrefix() {
        return snapShot + delimiter + getEntityPrefix();
    }

    public String buildSnapshotIndexKey() {
        return buildSnapshotPrefix() + delimiter + index;
    }

    /**
     * Provides entity prefix for example: interestRates
     * @return name of prefix which will be part of key
     */
    protected abstract String getEntityPrefix();

    public abstract String buildKey(T data, Context ctx);

    public abstract String getValueFromData(T data) throws IOException;
}
