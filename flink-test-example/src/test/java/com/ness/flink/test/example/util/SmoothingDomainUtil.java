package com.ness.flink.test.example.util;



import com.ness.flink.domain.Event;
import com.ness.flink.example.pipeline.domain.OptionPrice;
import com.ness.flink.example.pipeline.domain.SmoothingRequest;
import com.ness.flink.example.pipeline.domain.InterestRate;
import com.ness.flink.util.EventUtils;
import lombok.NoArgsConstructor;

import java.util.Objects;

@NoArgsConstructor
public final class SmoothingDomainUtil {

    public static SmoothingRequest merge(SmoothingRequest oldReq, SmoothingRequest newReq, boolean checkTimestamp) {
        if (!Objects.equals(oldReq.getUnderlying(), newReq.getUnderlying())) {
            throw new IllegalArgumentException("Cannot merge different underliers");
        }
        //update timestamp if it's newer
        updateTimestamp(oldReq, newReq);
        if (checkTimestamp) {
            newReq.getOptionPrices().forEach((instrumentId, newPrice) -> {
                final OptionPrice oldPrice = oldReq.getOptionPrices().get(instrumentId);
                if (updateRequired(oldPrice, newPrice)) {
                    oldReq.getOptionPrices().put(instrumentId, newPrice);
                }
            });

            newReq.getInterestRates().forEach((maturity, newRate) -> {
                final InterestRate oldRate = oldReq.getInterestRates().get(maturity);
                if (updateRequired(oldRate, newRate)) {
                    oldReq.getInterestRates().put(maturity, newRate);
                }
            });
        } else {
            oldReq.getOptionPrices().putAll(newReq.getOptionPrices());
            oldReq.getInterestRates().putAll(newReq.getInterestRates());
        }

        return oldReq;
    }

    private static void updateTimestamp(Event oldEvent, Event newEvent) {
        if (newEvent.getTimestamp() > oldEvent.getTimestamp()) {
            oldEvent.setTimestamp(newEvent.getTimestamp());
        }
    }

    private static boolean updateRequired(Event oldEvent, Event newEvent) {
        return EventUtils.updateRequired(oldEvent, newEvent);
    }
}
