package com.riskfocus.flink.example.pipeline.manager;

import com.riskfocus.flink.config.CheckpointingConfiguration;
import com.riskfocus.flink.config.EnvironmentConfiguration;
import com.riskfocus.flink.example.pipeline.config.JobMode;
import com.riskfocus.flink.example.pipeline.config.channel.ChannelProperties;
import com.riskfocus.flink.example.pipeline.config.channel.CustomerAccountChannelFactory;
import com.riskfocus.flink.example.pipeline.config.channel.OrderChannelFactory;
import com.riskfocus.flink.example.pipeline.domain.intermediate.CustomerAndAccount;
import com.riskfocus.flink.example.pipeline.manager.stream.CustomerAccountStream;
import com.riskfocus.flink.example.pipeline.manager.stream.OrdersStream;
import com.riskfocus.flink.util.ParamUtils;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
@Builder
public class FlinkJobManager {

    public static final String ERROR_TAG = "error-output";

    private final ParameterTool params;

    private final CustomerAccountChannelFactory customerAccountChannel;
    private final OrderChannelFactory orderChannelFactory;

    public void runJob() throws Exception {
        ParamUtils paramUtils = new ParamUtils(params);
        ChannelProperties channelProperties = new ChannelProperties(paramUtils);
        StreamExecutionEnvironment env = EnvironmentConfiguration.getEnvironment(paramUtils);
        // Register parameters https://ci.apache.org/projects/flink/flink-docs-stable/dev/best_practices.html
        env.getConfig().setGlobalJobParameters(params);

        CheckpointingConfiguration.configure(paramUtils, env);
        JobMode jobMode = channelProperties.getJobMode();

        DataStream<CustomerAndAccount> customerAndAccountDataStream = CustomerAccountStream
                .build(paramUtils, env, channelProperties, customerAccountChannel).build(env);
        OrdersStream.build(paramUtils, env, orderChannelFactory, customerAndAccountDataStream);

        log.debug("Job name: {}", jobMode.getJobName());
        log.debug("Execution Plan: {}", env.getExecutionPlan());
        env.execute(jobMode.getJobName());
    }

}
