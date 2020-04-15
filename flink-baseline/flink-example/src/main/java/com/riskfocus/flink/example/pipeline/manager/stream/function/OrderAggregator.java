package com.riskfocus.flink.example.pipeline.manager.stream.function;

import com.riskfocus.flink.example.pipeline.domain.Order;
import com.riskfocus.flink.example.pipeline.domain.Purchase;
import com.riskfocus.flink.example.pipeline.domain.intermediate.CustomerAndAccountAndOrder;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Khokhlov Pavel
 */
public class OrderAggregator implements AggregateFunction<CustomerAndAccountAndOrder, Purchase, Purchase> {

    @Override
    public Purchase createAccumulator() {
        List<Order> orders = new ArrayList<>();
        return Purchase.builder().orders(orders).build();
    }

    @Override
    public Purchase add(CustomerAndAccountAndOrder value, Purchase accumulator) {
        accumulator.setCustomer(value.getCustomer());
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
