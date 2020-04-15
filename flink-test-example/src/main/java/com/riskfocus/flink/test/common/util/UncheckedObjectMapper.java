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