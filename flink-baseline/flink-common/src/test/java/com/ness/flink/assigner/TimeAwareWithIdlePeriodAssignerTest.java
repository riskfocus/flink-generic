package com.ness.flink.assigner;

import com.ness.flink.domain.TimeAware;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Khokhlov Pavel
 */
class TimeAwareWithIdlePeriodAssignerTest {

    @Test
    void testAssigner() {

        TimeAwareWithIdlePeriodAssigner<TimeAware> assigner = new TimeAwareWithIdlePeriodAssigner<>(2_000, 0);

        Watermark currentWatermark = assigner.getCurrentWatermark();
        Assertions.assertEquals(Long.MIN_VALUE, currentWatermark.getTimestamp(),"We should get initial value");

        long timestamp = assigner.extractTimestamp(() -> 0, 0);

        Assertions.assertEquals(0, timestamp);

        currentWatermark = assigner.getCurrentWatermark();
        Assertions.assertEquals(currentWatermark.getTimestamp(), timestamp);

        timestamp = assigner.extractTimestamp(() -> 10, 0);

        currentWatermark = assigner.getCurrentWatermark();
        Assertions.assertEquals(currentWatermark.getTimestamp(), timestamp);


        timestamp = assigner.extractTimestamp(() -> -10, 0);
        Assertions.assertEquals(timestamp, -10);
        currentWatermark = assigner.getCurrentWatermark();
        Assertions.assertEquals(10, currentWatermark.getTimestamp(), "Watermark cannot go in past");

    }

    @Test
    void testAssignerInit() {
        TimeAwareWithIdlePeriodAssigner<TimeAware> assigner = new TimeAwareWithIdlePeriodAssigner<>(2_000, 0);

        long timestamp = assigner.extractTimestamp(() -> 2000, 0);
        Assertions.assertEquals(2000, timestamp);

        Watermark currentWatermark = assigner.getCurrentWatermark();
        Assertions.assertEquals(2000, currentWatermark.getTimestamp(), "We should't get initial value");
    }

}