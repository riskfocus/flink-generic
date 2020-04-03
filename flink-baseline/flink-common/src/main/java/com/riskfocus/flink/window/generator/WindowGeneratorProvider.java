package com.riskfocus.flink.window.generator;

import com.riskfocus.flink.window.WindowAware;
import com.riskfocus.flink.window.generator.impl.BasicGenerator;
import com.riskfocus.flink.util.ParamUtils;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.time.Duration;

/**
 * @author Khokhlov Pavel
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class WindowGeneratorProvider {

    public static final String WINDOW_SIZE_PARAM_NAME = "window.size.ms";
    public static final String WINDOW_PROVIDER_TYPE_PARAM_NAME = "window.provider.type";

    public static WindowAware create(ParamUtils params) {
        String batchTypeStr = params.getString(WINDOW_PROVIDER_TYPE_PARAM_NAME, GeneratorType.BASIC.name());
        long batchSize = params.getLong(WINDOW_SIZE_PARAM_NAME, Duration.ofSeconds(10).toMillis());
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
