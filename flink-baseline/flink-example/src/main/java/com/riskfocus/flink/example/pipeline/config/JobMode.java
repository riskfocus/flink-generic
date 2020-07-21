/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.example.pipeline.config;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author Khokhlov Pavel
 */
@Getter
@AllArgsConstructor
public enum JobMode {

    INTEREST_RATES_ONLY("smoothing-interest-rates"),
    OPTION_PRICES_ONLY("smoothing-option-prices"),
    FULL("smoothing-full");

    private final String jobName;
}
