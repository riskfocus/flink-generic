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
