/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author Khokhlov Pavel
 */
@Getter
@Setter
@NoArgsConstructor
public abstract class IncomingEvent implements Event {
    private static final long serialVersionUID = -4232714269203508651L;
    private long timestamp;
    private boolean eos;
}
