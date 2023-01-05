package com.ness.flink.util;

import com.ness.flink.domain.Event;
import com.ness.flink.domain.IncomingEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Khokhlov Pavel
 */
class EventUtilsTest {

    @Test
    void checkUpdateRequired() {

        Event previous = buildEvent(1);

        Event current = buildEvent(2);

        Assertions.assertTrue(EventUtils.updateRequired(previous, current));

        Event equalsTime = buildEvent(1);
        Assertions.assertTrue(EventUtils.updateRequired(previous, equalsTime));

        Event biggerTime = buildEvent(3);
        Assertions.assertTrue(EventUtils.updateRequired(previous, biggerTime));

        Event lessTime = buildEvent(0);
        Assertions.assertFalse(EventUtils.updateRequired(previous, lessTime));
    }

    private Event buildEvent(long timestamp) {
        Event price = new IncomingEvent() {
            private static final long serialVersionUID = 1329843453107167086L;
        };
        price.setTimestamp(timestamp);
        return price;
    }
}