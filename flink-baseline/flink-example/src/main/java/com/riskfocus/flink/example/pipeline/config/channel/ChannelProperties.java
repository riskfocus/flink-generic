package com.riskfocus.flink.example.pipeline.config.channel;

import com.riskfocus.flink.example.pipeline.config.JobMode;
import com.riskfocus.flink.util.ParamUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
@Getter
public class ChannelProperties {
    private final ParamUtils paramUtils;

    public boolean isInterestRatesKafkaSnapshotEnabled() {
        return paramUtils.getBoolean("interest.rates.kafka.snapshot.enabled", false);
    }

    public JobMode getJobMode() {
        return JobMode.valueOf(paramUtils.getString("pipeline.jobMode", JobMode.FULL.toString()));
    }
}
