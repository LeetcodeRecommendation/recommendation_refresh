package com.leetcoders.job_scheduling.utils;

import com.leetcoders.job_scheduling.database.PostgresHandler;
import com.leetcoders.job_scheduling.jobs.RecommendationRefreshRequest;
import com.leetcoders.job_scheduling.streams.KafkaStreams;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutionException;

@Configuration
public class ConfigBeans {
    @Bean
    PostgresHandler postgresHandler() {
        return new PostgresHandler(EnvironmentProviders.getPGServerIP(), EnvironmentProviders.getPGServerPort(),
                EnvironmentProviders.getPGUsername(),EnvironmentProviders.getPGPassword());
    }
    @Bean
    KafkaStreams kafkaStreams(PostgresHandler postgresHandler) throws ExecutionException, InterruptedException {
        return new KafkaStreams(EnvironmentProviders.getKafkaServers(), postgresHandler);
    }

    @Bean
    RecommendationRefreshRequest recommendationRefreshRequest(KafkaStreams kafkaStreams,
                                                              PostgresHandler postgresHandler) {
        return new RecommendationRefreshRequest(postgresHandler, kafkaStreams);
    }
}
