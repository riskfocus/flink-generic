package com.ness.flink.test.example.config;

import com.ness.flink.example.pipeline.domain.SmoothingRequest;
import com.ness.flink.test.common.metrics.MetricsService;
import com.ness.flink.test.example.receiver.ResultProcessor;
import com.ness.flink.test.example.receiver.ResultService;
import com.ness.flink.test.example.util.SmoothingDomainUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.springframework.boot.actuate.autoconfigure.metrics.KafkaMetricsAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.web.tomcat.TomcatMetricsAutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Map;
import java.util.Properties;

/**
 * @author Khokhlov Pavel
 */
@Configuration
@ComponentScan(basePackages = {"com.ness.flink.test"})
@Import({TestProperties.class})
@EnableAutoConfiguration(exclude = {RedisAutoConfiguration.class, RedisRepositoriesAutoConfiguration.class, KafkaMetricsAutoConfiguration.class, TomcatMetricsAutoConfiguration.class})
@Slf4j
public class TestConfig {

    @Bean("expandedProducer")
    public KafkaProducer<String, byte[]> createProducer(KafkaProperties springKafkaProperties) {
        Map<String, Object> producerProperties = springKafkaProperties.buildProducerProperties();
        Properties props = new Properties();
        props.putAll(producerProperties);

        return new KafkaProducer<>(props);
    }

    @Bean
    public MetricsService metricsService() {
        return new MetricsService();
    }

    @Bean
    public KafkaStreams testConsumingStream(KafkaProperties springKafkaProperties,
                                     TestProperties testProperties,
                                     ResultService<SmoothingRequest> resultService) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder
                .stream(testProperties.getSmoothingInput(), Consumed.with(Serdes.String(), new JsonSerde<>(SmoothingRequest.class)))
                .process(() -> new ResultProcessor<>(resultService, (context, v) -> {
                    v.setTimestamp(context.timestamp());
                    return v;
                }));

        Map<String, Object> streamsProperties = springKafkaProperties.buildStreamsProperties();
        Properties properties = new Properties();
        properties.putAll(streamsProperties);

        log.info("Kafka stream properties: {}", streamsProperties);

        final KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);

        Runtime.getRuntime().addShutdownHook(new Thread("perf-test-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
            }
        });

        streams.cleanUp();
        streams.start();

        return streams;
    }

    @Bean
    public ResultService<SmoothingRequest> resultService(TestProperties properties) {
        //align wait interval to the real window duration + extra time
        long waitInterval = properties.getOptionPricesWindowDurationMs() + properties.getWaitExtraDurationMs();
        return new ResultService<>(properties.isStrictWindowCheck(), waitInterval,
                (oldReq, newReq) -> SmoothingDomainUtil.merge(oldReq, newReq, properties.isCheckTimestamp())
        );
    }

}
