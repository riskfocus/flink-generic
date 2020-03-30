package com.riskfocus.flink.batch.generator;

import com.riskfocus.flink.batch.BatchAware;
import com.riskfocus.flink.batch.generator.impl.BasicGenerator;
import com.riskfocus.flink.util.ParamUtils;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.time.Duration;

/**
 * @author Khokhlov Pavel
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class BatchGeneratorProvider {

    public static final String BATCH_SIZE_PARAM_NAME = "batch.size.ms";
    public static final String BATCH_PROVIDER_TYPE_PARAM_NAME = "batch.provider.type";
    public static final String BATCH_MODE_PARAM_NAME = "batch";

    public static BatchAware create(ParamUtils params) {
        String batchTypeStr = params.getString(BATCH_PROVIDER_TYPE_PARAM_NAME, GeneratorType.BASIC.name());
        long batchSize = params.getLong(BATCH_SIZE_PARAM_NAME, Duration.ofSeconds(10).toMillis());
        final GeneratorType generatorType = GeneratorType.valueOf(batchTypeStr);
        switch (generatorType) {
            case BASIC:
                return new BasicGenerator(batchSize);
            case REST:
            default:
                throw new UnsupportedOperationException("Implementation required");
        }
    }
}
