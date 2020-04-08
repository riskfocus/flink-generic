package com.riskfocus.flink.example.pipeline.domain;

import com.riskfocus.flink.domain.IncomingEvent;
import lombok.*;

import java.math.BigDecimal;

/**
 * @author Khokhlov Pavel
 */
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class Order extends IncomingEvent {
    private static final long serialVersionUID = -7464403908757902162L;

    private int commodityId;
    private int customerId;
    private int quantity;
    private BigDecimal totalPrice;
}
