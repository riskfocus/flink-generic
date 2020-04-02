package com.riskfocus.flink.snapshot.redis;

import com.google.common.annotations.Beta;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

/**
 * @author Khokhlov Pavel
 */
@Beta
public class RedisSnapshotConverterUtils<T> implements Serializable {

    private static final long serialVersionUID = 7666812295659032901L;

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String DELIMITER = ":";

    static {
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    public SnapshotData<T> convertTo(Class<T> destClass, byte[] bytes) throws JsonProcessingException {
        String src = new String(bytes);
        int delimiterIdx = src.indexOf(DELIMITER);
        String contextIdStr = src.substring(0, delimiterIdx);
        String data = src.substring(delimiterIdx + 1);
        long contextId = Long.parseLong(contextIdStr);
        T t = mapper.readValue(data, destClass);
        return new SnapshotData<>(contextId, t);
    }

}