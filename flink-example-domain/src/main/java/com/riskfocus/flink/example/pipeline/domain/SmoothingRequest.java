package com.riskfocus.flink.example.pipeline.domain;

import com.riskfocus.flink.domain.IncomingEvent;
import com.riskfocus.flink.domain.KeyedAware;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper = false)
public class SmoothingRequest extends IncomingEvent implements KeyedAware {

    private Underlying underlying;
    /**
     * Uses instrumentId as key, to update with new prices. Should contain
     * only prices related to particular underlier
     */
    private Map<String, OptionPrice> optionPrices;
    /**
     * Uses maturity as key, to update with interest rate
     */
    private Map<String, InterestRate> interestRates;

    @Override
    public byte[] key() {
        return underlying.getName().getBytes();
    }
}
