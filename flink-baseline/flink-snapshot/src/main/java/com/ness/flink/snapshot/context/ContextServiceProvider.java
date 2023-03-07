//Copyright 2021-2023 Ness Digital Engineering
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.ness.flink.snapshot.context;

import com.ness.flink.config.properties.WatermarkProperties;
import com.ness.flink.snapshot.context.properties.ContextProperties;
import com.ness.flink.snapshot.context.rest.RestBased;
import com.ness.flink.window.generator.GeneratorType;
import com.ness.flink.window.WindowAware;
import com.ness.flink.window.generator.WindowGeneratorProvider;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * @author Khokhlov Pavel
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ContextServiceProvider {
    public static ContextService create(ContextProperties contextProperties, WatermarkProperties watermarkProperties) {
        final GeneratorType generatorType = contextProperties.getGeneratorType();
        WindowAware windowAware = WindowGeneratorProvider.create(watermarkProperties);
        switch (generatorType) {
            case BASIC:
                return new WindowBased(windowAware);
            case REST:
                return new RestBased(windowAware, contextProperties.getServiceUrl());
            default:
                throw new UnsupportedOperationException("Implementation required");
        }
    }
}
