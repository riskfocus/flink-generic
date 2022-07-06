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

package com.riskfocus.flink.assigner;

import com.riskfocus.flink.domain.TimeAware;

/**
 * @author Khokhlov Pavel
 */
public class TimeAwareWithIdlePeriodAssigner<T extends TimeAware> extends AscendingTimestampExtractorWithIdlePeriod<T> {

    private static final long serialVersionUID = 2121133396445336882L;

    public TimeAwareWithIdlePeriodAssigner(long maxIdlePeriod, long delay) {
        super(maxIdlePeriod, delay);
    }

    @Override
    public long extractTimestamp(TimeAware event) {
        return event.getTimestamp();
    }
}
