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

    public String buildSnapshotPrefix(ContextMetadata ctx) {
        return snapShot + delimiter + ctx.getContextName();
    }

    public String buildSnapshotIndexKey(ContextMetadata ctx) {
        return buildSnapshotPrefix(ctx) + delimiter + index;
    }

    public abstract String buildKey(T data, ContextMetadata ctx);

    public abstract String getValueFromData(T data) throws IOException;
}
