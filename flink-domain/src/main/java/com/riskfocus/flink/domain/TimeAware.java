/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.domain;

import java.io.Serializable;

/**
 * Inbound/Outbound message has to implement this interface
 *
 * @author Khokhlov Pavel
 */
@FunctionalInterface
public interface TimeAware extends Serializable {
    long getTimestamp();
}