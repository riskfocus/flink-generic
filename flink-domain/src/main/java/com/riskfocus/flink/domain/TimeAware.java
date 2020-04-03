package com.riskfocus.flink.domain;

import java.io.Serializable;

/**
 * @author Khokhlov Pavel
 */
@FunctionalInterface
public interface TimeAware extends Serializable {
    long getTimestamp();
}