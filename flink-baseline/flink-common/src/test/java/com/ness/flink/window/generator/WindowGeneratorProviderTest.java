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
package com.ness.flink.window.generator;


import com.ness.flink.config.properties.WatermarkProperties;
import com.ness.flink.window.WindowAware;
import com.ness.flink.window.WindowContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import java.util.Map;


/**
 * @author Khokhlov Pavel
 */
class WindowGeneratorProviderTest {
    @Test
    void shouldGenerateWindowContext() {
        WatermarkProperties watermarkProperties = WatermarkProperties.from(
            ParameterTool.fromMap(Map.of("watermark.windowSizeMs", "10000")));
        WindowAware windowAware = WindowGeneratorProvider.create(watermarkProperties);
        WindowContext windowContext = windowAware.generateWindowPeriod(System.currentTimeMillis());
        Assertions.assertEquals(10_000, windowContext.duration());
    }

}