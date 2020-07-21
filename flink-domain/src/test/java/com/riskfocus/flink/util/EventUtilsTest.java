/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.util;

import com.riskfocus.flink.domain.Event;
import com.riskfocus.flink.domain.IncomingEvent;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Khokhlov Pavel
 */
public class EventUtilsTest {

    @Test
    public void checkUpdateRequired() {

        Event previous = buildEvent(1);

        Event current = buildEvent(2);

        Assert.assertTrue(EventUtils.updateRequired(previous, current));

        Event equalsTime = buildEvent(1);
        Assert.assertTrue(EventUtils.updateRequired(previous, equalsTime));

        Event biggerTime = buildEvent(3);
        Assert.assertTrue(EventUtils.updateRequired(previous, biggerTime));

        Event lessTime = buildEvent(0);
        Assert.assertFalse(EventUtils.updateRequired(previous, lessTime));

    }

    private Event buildEvent(long timestamp) {
        Event price = new IncomingEvent() {

        };
        price.setTimestamp(timestamp);
        return price;
    }
}