package com.riskfocus.flink.example.pipeline.domain;

import com.riskfocus.flink.domain.IncomingEvent;
import com.riskfocus.flink.domain.KeyedAware;
import lombok.*;

import java.util.Collection;

/**
 * Pipeline result
 *
 * @author Khokhlov Pavel
 */
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class Purchase extends IncomingEvent implements KeyedAware {
    private static final long serialVersionUID = -7464403908757902162L;

    private Customer customer;

    private Collection<Order> orders;

    @Override
    public byte[] key() {
        return String.valueOf(customer.getCustomerId()).getBytes();
    }
}