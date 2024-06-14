/*
 * Copyright 2021-2023 Ness Digital Engineering
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
        return InterestRate.builder().interestRateId(rateCounter.getAndIncrement()).maturity(maturity).rate(rate).
            timestamp(timestamp).build();
    }
}
