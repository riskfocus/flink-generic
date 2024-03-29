/*
 * Copyright 2021-2023 Ness Digital Engineering
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ness.flink.storage.cache;

import com.google.common.annotations.Beta;

@Beta
public enum EntityTypeEnum {

    MEM_CACHE_ONLY(":"),
    STORAGE_ONLY("/"),
    MEM_CACHE_BACKED_BY_STORAGE("/"),
    MEM_CACHE_WITH_INDEX_SUPPORT_ONLY(":");

    private final String delimiter;

    EntityTypeEnum(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getDelimiter() {
        return delimiter;
    }
}
