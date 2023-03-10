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

package com.ness.flink.test.example.util;

import com.ness.flink.example.pipeline.domain.OptionPrice;
import com.ness.flink.example.pipeline.domain.SmoothingRequest;
import com.ness.flink.test.example.sender.ExpectedResultHolder;
import com.ness.flink.domain.IncomingEvent;
import com.ness.flink.example.pipeline.domain.InterestRate;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;

import java.util.Comparator;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Slf4j
public class CheckUtil {

    public static void checkAggregatedPrices(Map<String, Map<String, OptionPrice>> expectedPrices,
                                             Map<String, SmoothingRequest> actualResults) {
        expectedPrices.forEach((underlying, expectedOptionPrices) -> {
            SmoothingRequest actualRequest = actualResults.get(underlying);
            Assert.assertNotNull(actualRequest, "Cannot find option prices for Underlying: " + underlying);
            Map<String, OptionPrice> actualOptionPrices = actualRequest.getOptionPrices();
            checkPrices(actualOptionPrices, expectedOptionPrices);
        });
    }

    public static void checkPrices(Map<String, OptionPrice> actual, Map<String, OptionPrice> expected) {
        expected.forEach((instrumentId, price) ->
                assertEquals(actual.get(instrumentId), price, "Got wrong price value for underlier " + price.getUnderlying().getName())
        );
    }

    public static void checkInterestRates(Map<String, InterestRate> actualInterestRates, Map<String, InterestRate> expectedInterestRates, String windowId) {
        expectedInterestRates.forEach((expectedMaturity, expectedRate) -> {
            final InterestRate actualInterestRate = actualInterestRates.get(expectedMaturity);
            Assert.assertNotNull(actualInterestRate,
                    "Cannot find expected interestRate by expectedMaturity: " + expectedMaturity + " windowId: " + windowId);
            Assert.assertEquals(actualInterestRate, expectedRate, "Failed to compare InterestRates windowId: " + windowId);
        });
    }

    public static void checkResults(Map<String, ExpectedResultHolder> expectedResults,
                                    Map<String, Map<String, SmoothingRequest>> actualResults,
                                    int numberOfWindows, int numberOfInterestRates) {
        assertEquals(expectedResults.keySet().size(), numberOfWindows);

        expectedResults.forEach((windowId, expectedResultHolder) -> {
            log.info("Checking: windowId={}", windowId);
            // Key: windowID-underlying
            Map<String, SmoothingRequest> underliersOfWindow = actualResults.get(windowId);
            assertNotNull(underliersOfWindow);
            underliersOfWindow.values().stream()
                    .max(Comparator.comparingLong(IncomingEvent::getTimestamp)).ifPresent(p -> {
                long consumeTime = p.getTimestamp();
                long sendTime = expectedResultHolder.getSendTime();
                log.info("Latency for windowId: {}, {} ms", windowId, consumeTime - sendTime);
            });

            Map<String, OptionPrice> allPrices = underliersOfWindow.values().stream()
                    .map(SmoothingRequest::getOptionPrices)
                    .reduce((map1, map2) -> {
                        map1.putAll(map2);
                        return map1;
                    })
                    .orElseThrow();

            checkPrices(allPrices, expectedResultHolder.getData());

            if (numberOfInterestRates > 0) {
                final Map<String, InterestRate> expectedRates = expectedResultHolder.getRates();
                underliersOfWindow.values().forEach(actual -> {
                    checkInterestRates(actual.getInterestRates(), expectedRates, windowId);
                });
            }
        });
    }
}
