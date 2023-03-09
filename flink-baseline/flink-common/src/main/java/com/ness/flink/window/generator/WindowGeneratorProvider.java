/*
 * Copyright 2021-2023 Ness Digital Engineering
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ness.flink.window.generator;

import com.ness.flink.config.properties.WatermarkProperties;
import com.ness.flink.window.WindowAware;
import com.ness.flink.window.generator.impl.BasicGenerator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;


/**
 * @author Khokhlov Pavel
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class WindowGeneratorProvider {

    /**
     * Creates Window Generator
     * @param watermarkProperties watermark parameters
     * @return Window Generator
     */
    public static WindowAware create(WatermarkProperties watermarkProperties) {
        if (GeneratorType.BASIC == watermarkProperties.getWindowGeneratorType()) {
            return new BasicGenerator(watermarkProperties.getWindowSizeMs());
        }
        throw new UnsupportedOperationException("Implementation required");
    }


    /**
     * @author Khokhlov Pavel
     */
    public enum GeneratorType {
        BASIC
    }
}
