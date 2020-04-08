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
public class Account extends IncomingEvent {
    private static final long serialVersionUID = 2903682028500313270L;

    private int accountId;
    private int customerId;
    private BigDecimal amount;

}
