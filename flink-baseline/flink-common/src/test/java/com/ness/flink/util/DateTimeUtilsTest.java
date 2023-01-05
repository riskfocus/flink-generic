package com.ness.flink.util;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Khokhlov Pavel
 */
class DateTimeUtilsTest {

    @Test
    void formatDate() {
        long dateTime = 1585064594344L;
        String date = DateTimeUtils.formatDate(dateTime);
        Assertions.assertEquals("20200324", date);
    }
}