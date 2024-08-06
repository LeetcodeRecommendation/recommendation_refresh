package com.leetcoders.job_scheduling.jobs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.leetcoders.job_scheduling.database.PostgresHandler;
import com.leetcoders.job_scheduling.streams.KafkaStreams;
import com.leetcoders.job_scheduling.streams.UserRecommendationRefreshRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;


@Component
public class RecommendationRefreshRequest {
    static final Logger log = LoggerFactory.getLogger(RecommendationRefreshRequest.class);
    static final long THIRTY_SEC_IN_MS = 30 * 1000;
    private final PostgresHandler pgHandler;
    private final KafkaStreams kafkaStreams;

    public RecommendationRefreshRequest(PostgresHandler pgHandler, KafkaStreams kafkaStreams) {
        this.pgHandler = pgHandler;
        this.kafkaStreams = kafkaStreams;
    }

    @Scheduled(fixedRate = THIRTY_SEC_IN_MS)
    public void refreshRecommendationsForRequiredMembers() {
        var pendingClients = pgHandler.getClientsPendingForExecution();
        for (var client : pendingClients) {
            if (pgHandler.tryToMarkClientAsBusy(client)) {
                log.info("Locked client {} for recommendation refresh", client);
                UserRecommendationRefreshRequest client_details = pgHandler.getClientDetails(client);
                if (client_details == null) {
                    log.warn(String.format("Failed to get user details for %s... request wont be executed", client));
                    pgHandler.releaseClient(client);
                } else {
                    try {
                        kafkaStreams.distributeParseRequest(client_details);
                    } catch (JsonProcessingException e) {
                        log.error(String.format("Failed to get send kafka event for %s due to %s", client,
                                e.getMessage()));
                        pgHandler.releaseClient(client);
                    }
                }
            }
        }
    }
}
