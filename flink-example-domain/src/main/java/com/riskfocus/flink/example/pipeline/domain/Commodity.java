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
public class Commodity extends IncomingEvent {
    private static final long serialVersionUID = -1671136932820371688L;

    private int commodityId;
    private String name;
    private int quantity;
    private BigDecimal price;
}
