package com.riskfocus.flink.batch.generator.impl;

import com.riskfocus.flink.batch.BatchAware;
import com.riskfocus.flink.batch.BatchContext;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Khokhlov Pavel
 */
public class BasicGeneratorTest {

    @Test
    public void generateWindowPeriod() {

        long windowSize = 10_000;
        BatchAware batchAware = new BasicGenerator(windowSize);

        BatchContext context = batchAware.generateWindowPeriod(1578037899903L);
        Assert.assertNotNull(context);
        long windowId = 157803790L;
        Assert.assertEquals(windowId, context.getId());
        Assert.assertEquals(1578037890000L, context.getStart());
        Assert.assertEquals(1578037900000L, context.getEnd());

        BatchContext startWindow = batchAware.generateWindowPeriod(1578037890000L);

        Assert.assertEquals(windowId, startWindow.getId());

        long nextWindow = context.getEnd();

        BatchContext contextNext = batchAware.generateWindowPeriod(nextWindow);
        Assert.assertEquals(windowId + 1, contextNext.getId());


    }

    @Test
    public void convertToTimestamp() {
        long windowSize = 10_000;
        BatchAware batchAware = new BasicGenerator(windowSize);
        long time = 1578037899903L;
        long windowId = batchAware.generateWindowPeriod(time).getId();
        long timestampConverted = batchAware.convertToTimestamp(windowId);
        // check that we got time which less then Window size
        Assert.assertTrue(Math.abs(timestampConverted - time) - windowSize < windowSize);
    }

}