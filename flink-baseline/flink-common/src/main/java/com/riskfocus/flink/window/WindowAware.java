package com.riskfocus.flink.window;

/**
 * @author Khokhlov Pavel
 */
@FunctionalInterface
public interface WindowAware {

    /**
     * Generates window context
     *
     * @param timestamp unix timestamp of element
     * @return Window context
     */
    WindowContext generateWindowPeriod(long timestamp);
}
