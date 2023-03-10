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

package com.ness.flink.example.pipeline.config.sink.mapper;

import com.ness.flink.example.pipeline.domain.intermediate.InterestRates;
import com.ness.flink.json.UncheckedObjectMapper;
import com.ness.flink.snapshot.SnapshotMapper;
import com.ness.flink.snapshot.context.ContextMetadata;

/**
 * @author Khokhlov Pavel
 */
public class InterestRatesMapper extends SnapshotMapper<InterestRates> {

    private static final long serialVersionUID = 5743359314554830513L;

    public InterestRatesMapper(String delimiter) {
        super(delimiter);
    }

    @Override
    public String buildKey(InterestRates data, ContextMetadata context) {
        long contextId = context.getId();
        return buildSnapshotPrefix(context) + delimiter + context.getDate() + delimiter + contextId +
            delimiter + data.getCurrency();
    }

    @Override
    public String getValueFromData(InterestRates data) {
        return UncheckedObjectMapper.MAPPER.writeValueAsString(data);
    }

}
