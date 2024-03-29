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

package com.ness.flink.test.example;

import com.ness.flink.example.pipeline.domain.InterestRate;
import com.ness.flink.example.pipeline.domain.OptionPrice;
import com.ness.flink.example.pipeline.domain.SmoothingRequest;
import com.ness.flink.test.example.config.TestConfig;
import com.ness.flink.test.example.config.TestProperties;
import com.ness.flink.test.example.sender.ExpectedResultHolder;
import com.ness.flink.test.example.sender.SmoothingMessageSender;
import com.ness.flink.test.example.util.CheckUtil;
import com.ness.flink.util.DateTimeUtils;
import com.ness.flink.domain.IncomingEvent;
import com.ness.flink.test.example.receiver.ConsumerResult;
import com.ness.flink.test.example.receiver.ResultService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.testng.Assert.fail;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
@SpringBootTest
@ContextConfiguration(classes = {TestConfig.class})
public class SmoothingIT extends AbstractTestNGSpringContextTests {

    public static final String LATEST = "LATEST";

    @Autowired
    private TestProperties testProperties;
    @Autowired
    private SmoothingMessageSender messageSender;
    @Autowired
    private KafkaStreams testConsumingStream;
    @Autowired
    private ResultService<SmoothingRequest> resultService;

    @Test
    public void shouldReceiveLatestPrices() throws Exception {
        //define initial data with empty maps
        ExpectedResultHolder expectedInitial = new ExpectedResultHolder();
        if (testProperties.isSendInitialPrices()) {
            //according to the business requirements, if no interest rates arrive,
            // we send only those underliers where prices have been changed within a window. So no need to check initial prices
            if (testProperties.getNumberOfInterestRates() == 0) {
                throw new IllegalArgumentException("Number of Interest Rates = 0. " +
                        "It doesn't correspond with Send Initial Prices = true because initial prices won't be counted");
            }

            expectedInitial = messageSender.sendInitialMessages();

            log.info("Waiting for services to process initial prices&rates...");
            checkPriesAndRates(Map.of(expectedInitial.getKey(), expectedInitial));
            log.info("All initial prices&rates have been recieved and verified");
        }

        Map<String, ExpectedResultHolder> expectedResults = messageSender.sendMessages(expectedInitial);
        messageSender.close();

        log.info("Waiting for services to process prices&rates...");
        checkPriesAndRates(expectedResults);
    }

    private void checkPriesAndRates(Map<String, ExpectedResultHolder> expectedResults) throws Exception {
        if (expectedResults.size() > 0) {
            expectedResults.forEach((w, prices) -> log.info("Size per window: id={}, count={}", w, prices.getData().size()));
        } else {
            fail("No expected results");
        }

        while (!testConsumingStream.state().isRunningOrRebalancing()) {
            Thread.sleep(200);
            log.debug("Waiting until consumption stream starts...");
        }

        ConsumerResult<SmoothingRequest> consumerResult = resultService.getResult();
        Map<String, SmoothingRequest> actualResults = consumerResult.getRes();

        if (testProperties.isStrictWindowCheck()) {
            Set<String> duplicatedWindows = consumerResult.getDuplicatedWindows();
            Assert.assertEquals(duplicatedWindows.size(), 0, "Got duplicated Windows, Underlying already received: " +
                    String.join(",", duplicatedWindows));

            // windowId -> map of underlier to request
            Map<String, Map<String, SmoothingRequest>> results = actualResults.entrySet().stream().collect(
                    Collectors.groupingBy(e -> stripWindowId(e.getKey()), Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );
            CheckUtil.checkResults(expectedResults, results, testProperties.getNumberOfWindows(), testProperties.getNumberOfInterestRates());
        } else {
            ExpectedResultHolder expectedResultHolder = expectedResults.get(LATEST);
            Map<String, Map<String, OptionPrice>> expectedPricesByUnderlier = expectedResultHolder.getData().entrySet().stream().collect(
                    Collectors.groupingBy(e -> e.getValue().getUnderlying().getName(), Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );

            log.info("Checking prices&rates...");
            CheckUtil.checkAggregatedPrices(expectedPricesByUnderlier, actualResults);
            if (testProperties.getNumberOfInterestRates() > 0) {
                final Map<String, InterestRate> expectedRates = expectedResultHolder.getRates();
                actualResults.values().forEach(actual -> {
                    CheckUtil.checkInterestRates(actual.getInterestRates(), expectedRates, LATEST);
                });
            }

            long sendTime = expectedResultHolder.getSendTime();
            actualResults.values().stream()
                    .max(Comparator.comparingLong(IncomingEvent::getTimestamp))
                    .ifPresent(p -> {
                        long maxConsumeTime = p.getTimestamp();
                        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss,SSS");
                        String sendTimeStr = DateTimeUtils.format(sendTime, dateTimeFormatter);
                        String consumerTimeStr = DateTimeUtils.format(maxConsumeTime, dateTimeFormatter);
                        log.info("Latest window latency: {} ms, (last send time: {}, last consumed time: {})", maxConsumeTime - sendTime,
                                sendTimeStr, consumerTimeStr);
                    });
        }
    }

    // we better get an exception rather than let test get unpredictable results
    private String stripWindowId(String key) {
        return key.substring(0, key.indexOf('-'));
    }

}
