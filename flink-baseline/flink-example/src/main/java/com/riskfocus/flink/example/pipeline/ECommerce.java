package com.riskfocus.flink.example.pipeline;

import com.riskfocus.flink.example.pipeline.config.channel.CustomerAccountChannelFactory;
import com.riskfocus.flink.example.pipeline.config.channel.OrderChannelFactory;
import com.riskfocus.flink.example.pipeline.manager.FlinkJobManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

import java.time.ZoneId;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
public class ECommerce {

    @SuppressWarnings("java:S4823")
    public static void main(String[] args) throws Exception {
        log.info("Time zone is: {}", ZoneId.systemDefault());

        ParameterTool params = ParameterTool.fromArgs(args);
        log.info("Job params: {}", params.toMap());

        FlinkJobManager.builder()
                .params(params)
                .customerAccountChannel(new CustomerAccountChannelFactory())
                .orderChannelFactory(new OrderChannelFactory())
                .build().runJob();

    }
}
