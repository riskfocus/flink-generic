/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.example.snapshot.redis.domain;

import com.riskfocus.flink.domain.TimeAware;
import lombok.*;

/**
 * @author Khokhlov Pavel
 */
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class SimpleCurrency implements TimeAware {
    private long timestamp;
    private String code;
    private double rate;
}
