package com.riskfocus.flink.example.pipeline.domain.intermediate;

import com.riskfocus.flink.domain.IncomingEvent;
import com.riskfocus.flink.domain.KeyedAware;
import com.riskfocus.flink.example.pipeline.domain.Account;
import com.riskfocus.flink.example.pipeline.domain.Customer;
import com.riskfocus.flink.example.pipeline.domain.Order;
import lombok.*;

/**
 * @author Khokhlov Pavel
 */
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class CustomerAndAccountAndOrder extends IncomingEvent implements KeyedAware {

    private Customer customer;
    private Account account;
    private Order order;

    @Override
    public byte[] key() {
        return String.valueOf(customer.getCustomerId()).getBytes();
    }
}
