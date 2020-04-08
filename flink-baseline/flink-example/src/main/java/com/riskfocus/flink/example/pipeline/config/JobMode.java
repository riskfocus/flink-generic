package com.riskfocus.flink.example.pipeline.config;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author Khokhlov Pavel
 */
@Getter
@AllArgsConstructor
public enum JobMode {

    CUSTOMER_ACCOUNT("e-commerce-customer-account-join"),
    FULL("e-commerce-full");

    private final String jobName;
}
