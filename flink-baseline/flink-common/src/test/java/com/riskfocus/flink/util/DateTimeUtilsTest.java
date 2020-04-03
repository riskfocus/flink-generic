package com.riskfocus.flink.util;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Khokhlov Pavel
 */
public class DateTimeUtilsTest {

    @Test
    public void formatDate() {
        long dateTime = 1585064594344L;
        String date = DateTimeUtils.formatDate(dateTime);
        Assert.assertEquals("20200324", date);
    }
}