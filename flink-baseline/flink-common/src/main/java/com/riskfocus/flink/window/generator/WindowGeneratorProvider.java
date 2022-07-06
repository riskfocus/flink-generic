/*
 * Copyright 2020-2022 Ness USA, Inc.
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
        String windowProvider = params.getString(WINDOW_PROVIDER_TYPE_PARAM_NAME, GeneratorType.BASIC.name());
        long windowSize = getWindowSize(params);
        final GeneratorType generatorType = GeneratorType.valueOf(windowProvider);
        switch (generatorType) {
            case BASIC:
                return new BasicGenerator(windowSize);
            case REST:
            default:
                throw new UnsupportedOperationException("Implementation required");
        }
    }

    public static long getWindowSize(ParamUtils params) {
        return params.getLong(WINDOW_SIZE_PARAM_NAME, Duration.ofSeconds(10).toMillis());
    }
}
