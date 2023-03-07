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

package com.ness.flink.example.pipeline.domain;

import com.ness.flink.domain.IncomingEvent;
import com.ness.flink.domain.KafkaKeyedAware;
import lombok.*;
import lombok.experimental.SuperBuilder;

/**
 * @author Khokhlov Pavel
 */
@SuperBuilder
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class OptionPrice extends IncomingEvent implements KafkaKeyedAware {
    private static final long serialVersionUID = 5046115679955871440L;

    private String instrumentId;
    private Underlying underlying;
    private double price;

    @Override
    public String toString() {
        return '{' + "instrumentId=" + instrumentId + ", price=" + price + ", timestamp=" + getTimestamp() + '}';
    }

    @Override
    public byte[] kafkaKey() {
        return instrumentId.getBytes();
    }
}
