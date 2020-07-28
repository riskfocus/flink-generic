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

package com.riskfocus.flink.assigner;

import java.util.function.Function;

/**
 * Timestamp assigner with custom {@code Function<T, Long> timestampExtractor} function.
 * Made for a classes where timestamp is not defined by an interface and should be extracted manually.
 * <br>Suitable for Avro classes for example
 * <br>When initialized, <i>timestampExtractor</i> function should be defined as <b>Serializable</b>, for example:
 * <br>{@code (Function<TradeDto, Long> & Serializable) TradeDto::getTimestamp}
 * @author NIakovlev
 */
public class TimestampWithIdlePeriodAssigner<T> extends AscendingTimestampExtractorWithIdlePeriod<T> {

    private static final long serialVersionUID = 2121133396445336882L;

    private final Function<T, Long> timestampExtractor;

    public TimestampWithIdlePeriodAssigner(Function<T, Long> timestampExtractor, long maxIdlePeriod, long delay) {
        super(maxIdlePeriod, delay);
        this.timestampExtractor = timestampExtractor;
    }

    @Override
    public long extractTimestamp(T event) {
        return timestampExtractor.apply(event);
    }
}
