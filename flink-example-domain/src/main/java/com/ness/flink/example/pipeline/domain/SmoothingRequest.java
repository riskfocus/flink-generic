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

package com.ness.flink.example.pipeline.domain;

import com.ness.flink.domain.KafkaKeyedAware;
import com.ness.flink.domain.IncomingEvent;
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
public class SmoothingRequest extends IncomingEvent implements KafkaKeyedAware {
    private static final long serialVersionUID = -840147729638820749L;
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
    public byte[] kafkaKey() {
        return underlying.getName().getBytes();
    }
}
