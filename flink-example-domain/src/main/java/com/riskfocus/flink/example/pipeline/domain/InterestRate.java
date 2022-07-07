/*
 * Copyright 2020-2022 Ness USA, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.riskfocus.flink.example.pipeline.domain;

import com.riskfocus.flink.domain.IncomingEvent;
import com.riskfocus.flink.domain.KafkaKeyedAware;
import lombok.*;

/**
 * @author Khokhlov Pavel
 */
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
@EqualsAndHashCode(callSuper = false)
public class InterestRate extends IncomingEvent implements KafkaKeyedAware {

    private static final long serialVersionUID = 8613281459382114246L;

    private String maturity;
    private Double rate;

    @Override
    public byte[] kafkaKey() {
        return maturity.getBytes();
    }
}
