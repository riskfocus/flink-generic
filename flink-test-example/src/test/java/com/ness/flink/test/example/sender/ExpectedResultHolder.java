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

package com.ness.flink.test.example.sender;


import com.ness.flink.example.pipeline.domain.OptionPrice;
import com.ness.flink.example.pipeline.domain.InterestRate;
import lombok.Data;

import java.util.*;

/**
 * @author Khokhlov Pavel
 */
@Data
public class ExpectedResultHolder {
    private long sendTime, count;
    private String key;
    private Map<String, OptionPrice> data = new HashMap<>();
    private Map<String, InterestRate> rates = new HashMap<>();

    public static ExpectedResultHolder of(String windowId, long sendTime, long count, Map<String, OptionPrice> data, Map<String, InterestRate> rates) {
        ExpectedResultHolder expectedResultHolder = new ExpectedResultHolder();
        expectedResultHolder.setKey(windowId);
        expectedResultHolder.setSendTime(sendTime);
        expectedResultHolder.setCount(count);
        if (data != null) {
            expectedResultHolder.setData(data);
        }
        if (rates != null) {
            expectedResultHolder.setRates(rates);
        }
        return expectedResultHolder;
    }
}
