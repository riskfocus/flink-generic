//Copyright 2021-2023 Ness Digital Engineering
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.ness.flink.example.pipeline.domain.intermediate;

import com.ness.flink.domain.Event;
import com.ness.flink.domain.KafkaKeyedAware;
import com.ness.flink.domain.IncomingEvent;
import com.ness.flink.example.pipeline.domain.InterestRate;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Khokhlov Pavel
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
@EqualsAndHashCode(callSuper = false)
public class InterestRates extends IncomingEvent implements KafkaKeyedAware {
    private static final long serialVersionUID = -2395400450763583099L;

    public static final InterestRates EMPTY_RATES = new InterestRates();

    // Now just hardcoded
    private String currency = "USD";

    private Map<String, InterestRate> rates = new HashMap<>();

    public void add(InterestRate interestRate) {
        storeTimestamp(interestRate);
        rates.put(interestRate.getMaturity(), interestRate);
    }

    public boolean empty() {
        return rates.isEmpty();
    }

    private void storeTimestamp(Event event) {
        final long elementTime = event.getTimestamp();
        if (elementTime > getTimestamp()) {
            setTimestamp(elementTime);
        }
    }

    @Override
    public byte[] kafkaKey() {
        return currency.getBytes();
    }
}

