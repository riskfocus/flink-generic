//Copyright 2021-2023 Ness Digital Engineering
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.ness.flink.snapshot;

import com.ness.flink.snapshot.context.ContextMetadata;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
public abstract class SnapshotMapper<T> implements Serializable {
    private static final long serialVersionUID = -4643297908380913314L;

    private static final String SNAPSHOT_PREFIX = "snapshot";
    private static final String INDEX_PREFIX = "index";

    protected final String delimiter;

    /**
     * Provides prefix for snapshot
     * @param ctx metadata context
     * @return
     */
    public String buildSnapshotPrefix(ContextMetadata ctx) {
        return SNAPSHOT_PREFIX + delimiter + ctx.getContextName();
    }

    /**
     * Build index name for that specific data
     * Some storage types might support indexes as well (e.g: Redis)
     * @param ctx metadata context
     * @return name of index
     */
    public String buildSnapshotIndexKey(ContextMetadata ctx) {
        return buildSnapshotPrefix(ctx) + delimiter + INDEX_PREFIX;
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
