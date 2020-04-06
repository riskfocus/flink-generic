package com.riskfocus.flink.snapshot.context;

import com.riskfocus.flink.snapshot.context.rest.RestBased;
import com.riskfocus.flink.util.ParamUtils;
import com.riskfocus.flink.window.WindowAware;
import com.riskfocus.flink.window.generator.GeneratorType;
import com.riskfocus.flink.window.generator.WindowGeneratorProvider;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * @author Khokhlov Pavel
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ContextServiceProvider {

    public static final String CONTEXT_PROVIDER_TYPE_PARAM_NAME = "context.provider.type";
    public static final String CONTEXT_SERVICE_URL_PARAM_NAME = "context.service.url";

    public static ContextService create(ParamUtils params) {
        String batchTypeStr = params.getString(CONTEXT_PROVIDER_TYPE_PARAM_NAME, GeneratorType.BASIC.name());
        final GeneratorType generatorType = GeneratorType.valueOf(batchTypeStr);
        WindowAware windowAware = WindowGeneratorProvider.create(params);
        switch (generatorType) {
            case BASIC:
                return new WindowBased(windowAware);
            case REST:
                String url = params.getString(CONTEXT_SERVICE_URL_PARAM_NAME, "http://test.example.com");
                return new RestBased(windowAware, url);
            default:
                throw new UnsupportedOperationException("Implementation required");
        }
    }
}
