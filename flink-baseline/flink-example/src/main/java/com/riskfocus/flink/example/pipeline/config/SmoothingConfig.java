/*
 * Copyright 2020 Risk Focus Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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