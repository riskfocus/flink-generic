/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.test.example.data;


import com.riskfocus.flink.example.pipeline.domain.InterestRate;
import com.riskfocus.flink.example.pipeline.domain.OptionPrice;
import com.riskfocus.flink.example.pipeline.domain.Underlying;

import java.util.List;
import java.util.Random;

public class MockDataGenerator {

    private static final Random random = new Random();

    public static OptionPrice generateRandomPrice(List<String> underliers, int maxInstrumentId) {
        String underlier = underliers.get(random.nextInt(underliers.size()));
        String instrumentId = underlier + "_" + (1 + random.nextInt(maxInstrumentId));
        return generatePrice(underlier, instrumentId);
    }

    public static OptionPrice generatePrice(String underlier, String instrumentId) {
        return new OptionPrice(
                instrumentId,
                new Underlying(underlier),
                (random.nextInt(100) * random.nextDouble())
        );
    }

    public static InterestRate generateRandomRate(List<String> maturities) {
        String maturity = maturities.get(random.nextInt(maturities.size()));
        return generateRate(maturity);
    }

    public static InterestRate generateRate(String maturity) {
        double rate = (random.nextInt(100) * random.nextDouble());
        return InterestRate.builder().maturity(maturity).rate(rate).build();
    }
}
