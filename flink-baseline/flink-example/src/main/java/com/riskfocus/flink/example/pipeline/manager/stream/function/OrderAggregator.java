package com.riskfocus.flink.example.pipeline.manager.stream.function;

import com.riskfocus.flink.example.pipeline.domain.Purchase;
import com.riskfocus.flink.example.pipeline.domain.intermediate.CustomerAndAccountAndOrder;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author Khokhlov Pavel
 */
public class OrderAggregator implements AggregateFunction<CustomerAndAccountAndOrder, Purchase, Purchase> {

    @Override
    public Purchase createAccumulator() {
        return Purchase.builder().build();
    }

    @Override
    public Purchase add(CustomerAndAccountAndOrder value, Purchase accumulator) {
        accumulator.setTimestamp(value.getTimestamp());
        accumulator.getOrders().add(value.getOrder());
        return accumulator;
    }

    @Override
    public Purchase getResult(Purchase accumulator) {
        return accumulator;
    }

    @Override
    public Purchase merge(Purchase currentOne, Purchase anotherOne) {
        currentOne.getOrders().addAll(anotherOne.getOrders());
        return currentOne;
    }
}
