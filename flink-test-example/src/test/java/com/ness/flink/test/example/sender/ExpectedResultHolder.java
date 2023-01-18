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
