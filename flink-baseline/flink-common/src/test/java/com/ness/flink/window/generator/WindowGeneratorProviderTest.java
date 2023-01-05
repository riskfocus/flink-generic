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