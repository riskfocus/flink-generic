package com.riskfocus.flink.window;

/**
 * @author Khokhlov Pavel
 */
public interface WindowAware {

    /**
     * Generates window context
     *
     * @param timestamp unix timestamp of element
     * @return Window context
     */
    WindowContext generateWindowPeriod(long timestamp);

    /**
     * Back operation which converts provided windowId to unix timestamp
     *
     * @param windowId window identifier
     * @return timestamp
     */
    long convertToTimestamp(long windowId);

}
