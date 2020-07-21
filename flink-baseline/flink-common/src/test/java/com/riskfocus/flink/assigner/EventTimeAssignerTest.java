/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.assigner;

import com.riskfocus.flink.domain.TimeAware;
import lombok.Builder;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Khokhlov Pavel
 */
public class EventTimeAssignerTest {


    private EventTimeAssigner<TestEvent> testEventEventTimeAssigner = new EventTimeAssigner<>(0, 1000);


    @Test
    public void testGetCurrentWatermark() {
        Watermark watermark = testEventEventTimeAssigner.getCurrentWatermark();
        Assert.assertNotNull(watermark);
    }

    @Test
    public void testExtractTimestamp() {
        TestEvent testEvent = TestEvent.builder().timestamp(2L).build();
        long actual = testEventEventTimeAssigner.extractTimestamp(testEvent, 0);
        Assert.assertEquals(actual, 2L);
    }

    @Builder
    public static class TestEvent implements TimeAware {
        private static final long serialVersionUID = -3722944880902475723L;
        private long timestamp;

        @Override
        public long getTimestamp() {
            return timestamp;
        }
    }
}