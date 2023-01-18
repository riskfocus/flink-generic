package com.ness.flink.window.generator.impl;

import com.ness.flink.window.WindowAware;
import com.ness.flink.window.WindowContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Khokhlov Pavel
 */
class BasicGeneratorTest {

    @Test
    void generateWindowPeriod() {

        long windowSize = 10_000;
        WindowAware windowAware = new BasicGenerator(windowSize);

        WindowContext context = windowAware.generateWindowPeriod(1578037899903L);
        Assertions.assertNotNull(context);
        long windowId = 157803790L;
        Assertions.assertEquals(windowId, context.getId());
        Assertions.assertEquals(1578037890000L, context.getStart());
        Assertions.assertEquals(1578037900000L, context.getEnd());

        WindowContext startWindow = windowAware.generateWindowPeriod(1578037890000L);

        Assertions.assertEquals(startWindow.duration(), windowSize, "Size of the batch must be equals to provided settings");

        Assertions.assertEquals(windowId, startWindow.getId());

        long endOfBatch = context.endOfWindow();
        Assertions.assertEquals(windowAware.generateWindowPeriod(endOfBatch).getId(), windowId, "endOfBatch must be part of the same batch");

        long nextWindow = context.getEnd();

        WindowContext contextNext = windowAware.generateWindowPeriod(nextWindow);
        Assertions.assertEquals(windowId + 1, contextNext.getId());


    }

}