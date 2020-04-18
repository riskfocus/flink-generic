package com.riskfocus.flink.assigner;

import com.riskfocus.flink.domain.TimeAware;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Khokhlov Pavel
 */
public class TimeAwareWithIdlePeriodAssignerTest {

    @Test
    public void testAssigner() {

        TimeAwareWithIdlePeriodAssigner<TimeAware> assigner = new TimeAwareWithIdlePeriodAssigner<>(2_000, 0);

        Watermark currentWatermark = assigner.getCurrentWatermark();
        Assert.assertEquals(currentWatermark.getTimestamp(), Long.MIN_VALUE, "We should get initial value");

        long timestamp = assigner.extractTimestamp(() -> 0, 0);

        Assert.assertEquals(timestamp, 0);

        currentWatermark = assigner.getCurrentWatermark();
        Assert.assertEquals(currentWatermark.getTimestamp(), timestamp);

        timestamp = assigner.extractTimestamp(() -> 10, 0);

        currentWatermark = assigner.getCurrentWatermark();
        Assert.assertEquals(currentWatermark.getTimestamp(), timestamp);


        timestamp = assigner.extractTimestamp(() -> -10, 0);
        Assert.assertEquals(timestamp, -10);
        currentWatermark = assigner.getCurrentWatermark();
        Assert.assertEquals(currentWatermark.getTimestamp(), 10, "Watermark cannot go in past");

    }

    @Test
    public void testAssignerInit() {
        TimeAwareWithIdlePeriodAssigner<TimeAware> assigner = new TimeAwareWithIdlePeriodAssigner<>(2_000, 0);

        long timestamp = assigner.extractTimestamp(() -> 2000, 0);
        Assert.assertEquals(timestamp, 2000);

        Watermark currentWatermark = assigner.getCurrentWatermark();
        Assert.assertEquals(currentWatermark.getTimestamp(), 2000, "We should't get initial value");
    }

}