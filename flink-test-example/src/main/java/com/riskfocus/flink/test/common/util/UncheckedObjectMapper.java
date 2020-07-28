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

package com.riskfocus.flink.test.common.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;


public final class UncheckedObjectMapper extends ObjectMapper {

    @Override
    public <T> T readValue(String content, Class<T> valueType) {
        if (content == null) {
            return null;
        } else {
            try {
                return super.readValue(content, valueType);
            } catch (Exception var4) {
                throw new IllegalArgumentException(var4);
            }
        }
    }

    @Override
    public <T> T readValue(InputStream src, Class<T> valueType) {
        try {
            return super.readValue(src, valueType);
        } catch (Exception var4) {
            throw new IllegalArgumentException(var4);
        }
    }

    @Override
    public <T> T readValue(byte[] content, Class<T> valueType) {
        try {
            return super.readValue(content, valueType);
        } catch (Exception var4) {
            throw new IllegalArgumentException(var4);
        }
    }

    @Override
    public String writeValueAsString(Object value) {
        try {
            return super.writeValueAsString(value);
        } catch (JsonProcessingException var3) {
            throw new IllegalArgumentException(var3);
        }
    }

    @Override
    public byte[] writeValueAsBytes(Object value) {
        try {
            return super.writeValueAsBytes(value);
        } catch (JsonProcessingException var3) {
            throw new IllegalArgumentException(var3);
        }
    }
}