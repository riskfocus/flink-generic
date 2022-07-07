/*
 * Copyright 2020-2022 Ness USA, Inc.
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

package com.riskfocus.flink.util;

import com.riskfocus.flink.domain.TimeAware;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * @author Khokhlov Pavel
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class EventUtils {

    /**
     * Compare events based on timestamp
     * required for failover resilience
     *
     * We should update data in case of new event has bigger or equals date
     *
     * @param previous event which was received before (e.g. previous event can be received from the storage)
     * @param current current event
     * @return If updated required returns true otherwise returns false
     */
    public static boolean updateRequired(TimeAware previous, TimeAware current) {
        if (previous == null) {
            return true;
        }
        long previousTimestamp = previous.getTimestamp();
        long currentTimestamp = current.getTimestamp();
        return currentTimestamp >= previousTimestamp;
    }
}
