package com.riskfocus.flink.example.pipeline.config.sink.mapper;

import com.riskfocus.flink.example.pipeline.domain.intermediate.InterestRates;
import com.riskfocus.flink.snapshot.SnapshotMapper;
import com.riskfocus.flink.snapshot.context.ContextMetadata;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * @author Khokhlov Pavel
 */
public class InterestRatesMapper extends SnapshotMapper<InterestRates> {

    private static final long serialVersionUID = 5743359314554830513L;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public InterestRatesMapper(String delimiter) {
        super(delimiter);
    }

    @Override
    public String buildKey(InterestRates data, ContextMetadata context) {
        long contextId = context.getId();
        return buildSnapshotPrefix(context) + delimiter + context.getDate() + delimiter + contextId + delimiter + data.getCurrency();
    }

    @Override
    public String getValueFromData(InterestRates data) throws IOException {
        return objectMapper.writeValueAsString(data);
    }

}
