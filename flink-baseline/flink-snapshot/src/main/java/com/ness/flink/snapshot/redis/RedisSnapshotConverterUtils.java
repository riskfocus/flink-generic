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

package com.ness.flink.snapshot.redis;

import com.google.common.annotations.Beta;
import com.ness.flink.json.UncheckedObjectMapper;
import java.io.Serializable;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * @author Khokhlov Pavel
 */
@Beta
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RedisSnapshotConverterUtils implements Serializable {
    private static final long serialVersionUID = 7666812295659032901L;

    private static final String DELIMITER = ":";

    public static <T> SnapshotData<T> convertTo(Class<T> destClass, byte[] bytes) {
        String src = new String(bytes);
        int delimiterIdx = src.indexOf(DELIMITER);
        String contextIdStr = src.substring(0, delimiterIdx);
        String data = src.substring(delimiterIdx + 1);
        long contextId = Long.parseLong(contextIdStr);
        T element = UncheckedObjectMapper.MAPPER.readValue(data, destClass);
        return new SnapshotData<>(contextId, element);
    }

}