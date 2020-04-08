package com.riskfocus.flink.domain;

/**
 * @author Khokhlov Pavel
 */
public interface Event extends TimeAware {
    void setTimestamp(long timestamp);
    void setEos(boolean isEos);
    boolean isEos();
}
