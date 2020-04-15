package com.riskfocus.flink.test.example.data;

import com.riskfocus.flink.example.pipeline.domain.Account;
import com.riskfocus.flink.example.pipeline.domain.Commodity;
import com.riskfocus.flink.example.pipeline.domain.Customer;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Khokhlov Pavel
 */
public class MockDataGenerator {


    public static Collection<Customer> generateCustomers(int customerCount) {
        return IntStream.rangeClosed(1, customerCount).mapToObj(accId ->
                Customer.builder()
                        .customerId(accId)
                        .email(String.format("user-%d@mail.com", accId))
                        .name(String.format("Name of user-%d", accId))
                        .build()
        ).collect(Collectors.toList());
    }

    public static Collection<Account> generateAccounts(Collection<Customer> customers) {
        int initAmount = 100;
        Collection<Account> res = new ArrayList<>();
        for (Customer customer : customers) {
            Account acc = Account.builder()
                    .accountId(customer.getCustomerId())
                    .customerId(customer.getCustomerId())
                    .amount(new BigDecimal(initAmount))
                    .build();
            res.add(acc);
            initAmount++;
        }
        return res;
    }

    public static Collection<Commodity> generateCommodities(int commoditiesCount) {
        return IntStream.rangeClosed(1, commoditiesCount).mapToObj(accId -> Commodity.builder()
                .commodityId(accId)
                .name(String.format("Product name: %d", accId))
                .quantity(accId)
                .price(new BigDecimal(accId))
                .build()
        ).collect(Collectors.toList());
    }
}
