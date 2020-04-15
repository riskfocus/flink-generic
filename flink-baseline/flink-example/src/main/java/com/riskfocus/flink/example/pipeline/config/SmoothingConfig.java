package com.riskfocus.flink.example.pipeline.config;

import com.riskfocus.flink.storage.cache.EntityTypeEnum;
import com.riskfocus.flink.util.ParamUtils;
import lombok.AllArgsConstructor;

/**
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
public class SmoothingConfig {

    private final ParamUtils params;

    public EntityTypeEnum getSnapshotSinkType() {
        String snapshotType = params.getString("snapshot.type", EntityTypeEnum.MEM_CACHE_WITH_INDEX_SUPPORT_ONLY.name());
        return EntityTypeEnum.valueOf(snapshotType);
    }

}