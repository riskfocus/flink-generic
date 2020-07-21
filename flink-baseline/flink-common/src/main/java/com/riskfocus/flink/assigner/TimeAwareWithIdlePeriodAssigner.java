/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.assigner;

import com.riskfocus.flink.domain.TimeAware;

/**
 * @author Khokhlov Pavel
 */
public class TimeAwareWithIdlePeriodAssigner<T extends TimeAware> extends AscendingTimestampExtractorWithIdlePeriod<T> {

    private static final long serialVersionUID = 2121133396445336882L;

    public TimeAwareWithIdlePeriodAssigner(long maxIdlePeriod, long delay) {
        super(maxIdlePeriod, delay);
    }

    @Override
    public long extractTimestamp(TimeAware event) {
        return event.getTimestamp();
    }
}
