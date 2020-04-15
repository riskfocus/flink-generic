package com.riskfocus.flink.example.pipeline.domain.intermediate;

import com.riskfocus.flink.domain.Event;
import com.riskfocus.flink.domain.IncomingEvent;
import com.riskfocus.flink.domain.KeyedAware;
import com.riskfocus.flink.example.pipeline.domain.InterestRate;
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
public class InterestRates extends IncomingEvent implements KeyedAware {
    private static final long serialVersionUID = -2395400450763583099L;

    public static InterestRates EMPTY = new InterestRates();

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
    public byte[] key() {
        return currency.getBytes();
    }
}

