package com.riskfocus.flink.example.snapshot;

import com.riskfocus.flink.example.domain.SimpleCurrency;
import com.riskfocus.flink.snapshot.SnapshotMapper;
import com.riskfocus.flink.snapshot.context.ContextMetadata;
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
        long contextId = contextMetadata.getId();
        return buildSnapshotPrefix(contextMetadata) + delimiter + contextMetadata.getDate() + delimiter + contextId + delimiter + data.getCode();
    }

    @Override
    public String getValueFromData(SimpleCurrency data) throws IOException {
        return objectMapper.writeValueAsString(data);
    }
}