package com.ness.flink.test.example.data;


import com.ness.flink.example.pipeline.domain.OptionPrice;
import com.ness.flink.example.pipeline.domain.Underlying;
import com.ness.flink.example.pipeline.domain.InterestRate;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class MockDataGenerator {

    private static final Random random = new Random();

    private static AtomicInteger rateCounter = new AtomicInteger(0);

    public static OptionPrice generateRandomPrice(List<String> underliers, int maxInstrumentId, long timestamp) {
        String underlier = underliers.get(random.nextInt(underliers.size()));
        String instrumentId = underlier + "_" + (1 + random.nextInt(maxInstrumentId));
        return generatePrice(underlier, instrumentId, timestamp);
    }

    public static OptionPrice generatePrice(String underlier, String instrumentId, long timestamp) {
        return OptionPrice.builder().instrumentId(instrumentId)
            .underlying(new Underlying(underlier)).price(random.nextInt(100) * random.nextDouble())
            .timestamp(timestamp)
            .build();
    }

    public static InterestRate generateRandomRate(List<String> maturities, long timestamp) {
        String maturity = maturities.get(random.nextInt(maturities.size()));
        return generateRate(maturity, timestamp);
    }

    public static InterestRate generateRate(String maturity, long timestamp) {
        double rate = (random.nextInt(100) * random.nextDouble());
        return InterestRate.builder().id(rateCounter.getAndIncrement()).maturity(maturity).rate(rate).
            timestamp(timestamp).build();
    }
}
