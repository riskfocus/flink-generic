package com.riskfocus.flink.example.pipeline.config.sink.mapper;

import com.riskfocus.flink.example.pipeline.domain.intermediate.CustomerAndAccount;
import com.riskfocus.flink.snapshot.SnapshotMapper;
import com.riskfocus.flink.snapshot.context.ContextMetadata;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * @author Khokhlov Pavel
 */
public class CustomerAccountMapper extends SnapshotMapper<CustomerAndAccount> {
    private static final long serialVersionUID = -901565086238142875L;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public CustomerAccountMapper(String delimiter) {
        super(delimiter);
    }

    @Override
    public String buildKey(CustomerAndAccount data, ContextMetadata context) {
        return buildSnapshotPrefix(context) + delimiter + context.getDate() + delimiter + context.getId();
    }

    @Override
    public String getValueFromData(CustomerAndAccount data) throws IOException {
        return objectMapper.writeValueAsString(data);
    }

}
