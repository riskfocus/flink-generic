package com.riskfocus.flink.batch;

/**
 * @author Khokhlov Pavel
 */
public interface BatchAware {

    /**
     * Generates batch duration
     *
     * @param timestamp time of element
     * @return identifier
     */
    BatchContext generateWindowPeriod(long timestamp);

    /**
     * Back operation which converts WindowId to Timestamp
     * @param windowId window identity
     * @return timestamp
     */
    long convertToTimestamp(long windowId);

}
