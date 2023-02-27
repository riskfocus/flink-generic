package com.ness.flink.sink.jdbc.domain;

import com.ness.flink.domain.IncomingEvent;
import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/**
 * @author Khokhlov Pavel
 */
@SuperBuilder
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class Price extends IncomingEvent {
    private static final long serialVersionUID = 1L;

    private int id;
    private String sourceId;
    private BigDecimal value;
}
