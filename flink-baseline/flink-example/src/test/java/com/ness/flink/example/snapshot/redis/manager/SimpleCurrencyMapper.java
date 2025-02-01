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

package com.ness.flink.example.snapshot.redis.manager;

import com.ness.flink.example.snapshot.redis.domain.SimpleCurrency;
import com.ness.flink.snapshot.SnapshotMapper;
import com.ness.flink.snapshot.context.ContextMetadata;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;


import java.io.IOException;

/**
 * @author Khokhlov Pavel
 */
public class SimpleCurrencyMapper extends SnapshotMapper<SimpleCurrency> {

    private static final long serialVersionUID = 5743359314554830513L;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public SimpleCurrencyMapper(String delimiter) {
        super(delimiter);
    }

    @Override
    public String buildKey(SimpleCurrency data, ContextMetadata contextMetadata) {
        long contextId = contextMetadata.getContextId();
        return buildSnapshotPrefix(contextMetadata) + delimiter + contextMetadata.getDate() + delimiter + contextId + delimiter + data.getCode();
    }

    @Override
    public String getValueFromData(SimpleCurrency data) throws IOException {
        return objectMapper.writeValueAsString(data);
    }
}