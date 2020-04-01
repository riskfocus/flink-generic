package com.riskfocus.flink.window.generator.impl;

import com.riskfocus.flink.window.WindowAware;
import com.riskfocus.flink.window.WindowContext;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Khokhlov Pavel
 */
public class BasicGeneratorTest {

    @Test
    public void generateWindowPeriod() {

        long windowSize = 10_000;
        WindowAware windowAware = new BasicGenerator(windowSize);

        WindowContext context = windowAware.generateWindowPeriod(1578037899903L);
        Assert.assertNotNull(context);
        long windowId = 157803790L;
        Assert.assertEquals(windowId, context.getId());
        Assert.assertEquals(1578037890000L, context.getStart());
        Assert.assertEquals(1578037900000L, context.getEnd());

        WindowContext startWindow = windowAware.generateWindowPeriod(1578037890000L);

        Assert.assertEquals(startWindow.duration(), windowSize, "Size of the batch must be equals to provided settings");

        Assert.assertEquals(windowId, startWindow.getId());

        long endOfBatch = context.endOfWindow();
        Assert.assertEquals(windowAware.generateWindowPeriod(endOfBatch).getId(), windowId, "endOfBatch must be part of the same batch");

        long nextWindow = context.getEnd();

        WindowContext contextNext = windowAware.generateWindowPeriod(nextWindow);
        Assert.assertEquals(windowId + 1, contextNext.getId());


    }

    @Test
    public void convertToTimestamp() {
        long windowSize = 10_000;
        WindowAware windowAware = new BasicGenerator(windowSize);
        long time = 1578037899903L;
        long windowId = windowAware.generateWindowPeriod(time).getId();
        long timestampConverted = windowAware.convertToTimestamp(windowId);
        // check that we got time which less then Window size
        Assert.assertTrue(Math.abs(timestampConverted - time) - windowSize < windowSize);
    }

}