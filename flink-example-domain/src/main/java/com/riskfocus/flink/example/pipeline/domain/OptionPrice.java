package com.riskfocus.flink.example.pipeline.domain;

import com.riskfocus.flink.domain.IncomingEvent;
import com.riskfocus.flink.domain.KeyedAware;
import lombok.*;

/**
 * @author Khokhlov Pavel
 */
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class OptionPrice extends IncomingEvent implements KeyedAware {
    private static final long serialVersionUID = 5046115679955871440L;

    private String instrumentId;
    private Underlying underlying;
    private double price;

    @Override
    public String toString() {
        return '{' + "instrumentId=" + instrumentId + ", price=" + price + ", timestamp=" + getTimestamp() + '}';
    }

    @Override
    public byte[] key() {
        return instrumentId.getBytes();
    }
}
