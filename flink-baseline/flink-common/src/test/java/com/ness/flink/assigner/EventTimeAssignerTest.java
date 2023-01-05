package com.ness.flink.assigner;

import com.ness.flink.domain.TimeAware;
import lombok.Builder;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


/**
 * @author Khokhlov Pavel
 */
class EventTimeAssignerTest {

    final EventTimeAssigner<TestEvent> testEventEventTimeAssigner = new EventTimeAssigner<>(0, 1000);

    @Test
    void testGetCurrentWatermark() {
        Watermark watermark = testEventEventTimeAssigner.getCurrentWatermark();
        Assertions.assertNotNull(watermark);
    }

    @Test
    void testExtractTimestamp() {
        TestEvent testEvent = TestEvent.builder().timestamp(2L).build();
        long actual = testEventEventTimeAssigner.extractTimestamp(testEvent, 0);
        Assertions.assertEquals(actual, 2L);
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