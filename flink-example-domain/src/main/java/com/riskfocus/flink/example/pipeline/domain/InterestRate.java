/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.example.pipeline.domain;

import com.riskfocus.flink.domain.IncomingEvent;
import com.riskfocus.flink.domain.KeyedAware;
import lombok.*;

/**
 * @author Khokhlov Pavel
 */
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
@EqualsAndHashCode(callSuper = false)
public class InterestRate extends IncomingEvent implements KeyedAware {

    private static final long serialVersionUID = 8613281459382114246L;

    private String maturity;
    private Double rate;

    @Override
    public byte[] key() {
        return maturity.getBytes();
    }
}
