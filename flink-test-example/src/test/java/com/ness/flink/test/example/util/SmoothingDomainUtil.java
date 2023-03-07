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
