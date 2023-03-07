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

package com.ness.flink.json;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;

import java.io.InputStream;

/**
 * Provides a common preconfigured {@link ObjectMapper} instance, with all exceptions omitted for compiler.
 */
public final class UncheckedObjectMapper extends ObjectMapper {
    private static final long serialVersionUID = 3677114053325071289L;

    public static final UncheckedObjectMapper MAPPER = (UncheckedObjectMapper) new UncheckedObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    @SneakyThrows
    @Override
    public <T> T readValue(String content, Class<T> valueType) {
        if (content == null) {
            return null;
        } else {
            return super.readValue(content, valueType);
        }
    }

    @SneakyThrows
    @Override
    public <T> T readValue(InputStream src, Class<T> valueType) {
        return super.readValue(src, valueType);
    }

    @SneakyThrows
    @Override
    public <T> T readValue(byte[] content, Class<T> valueType) {
        return super.readValue(content, valueType);
    }

    @SneakyThrows
    @Override
    public String writeValueAsString(Object value) {
        return super.writeValueAsString(value);
    }

    @SneakyThrows
    @Override
    public byte[] writeValueAsBytes(Object value) {
        return super.writeValueAsBytes(value);
    }

}
