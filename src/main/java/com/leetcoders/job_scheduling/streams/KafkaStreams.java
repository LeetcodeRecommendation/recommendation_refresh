package com.leetcoders.job_scheduling.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.leetcoders.job_scheduling.database.PostgresHandler;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Component
public class KafkaStreams {
    private static final Logger log = LoggerFactory.getLogger(KafkaStreams.class);
    private static final String RECOMMENDATION_REQUEST_TOPIC = "rs_request_topic";
    private static final String RECOMMENDATION_RESPONSE_TOPIC = "rs_response_topic";
    private static final String CONSUMER_GROUP_ID = "response_group";
    private final KafkaProducer<String, String> topicProducer;

    public KafkaStreams(String kafkaServers, PostgresHandler databaseHandler){
        Thread consumingThread = new Thread(() -> {
            try {
                var topicConsumer = createKafkaConsumer(kafkaServers);
                log.info("Initialized kafka consumer");
                topicConsumer.subscribe(List.of(RECOMMENDATION_RESPONSE_TOPIC));
                consumeTopics(topicConsumer, databaseHandler);
            } catch (JsonProcessingException e) {
                log.error("Failed to consume topics! aborting...");
                System.exit(-1);
            }
        });
        consumingThread.start();

        topicProducer = createKafkaProducer(kafkaServers);
    }
    private KafkaProducer<String, String> createKafkaProducer(String kafkaServers) {
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(producerProperties);
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(String kafkaServers) {
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return new KafkaConsumer<>(consumerProperties);
    }

    public static void consumeTopics(KafkaConsumer<String, String> consumer, PostgresHandler databaseHandler)
            throws JsonProcessingException {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                final ObjectMapper mapper = new ObjectMapper();
                UserRecommendationRefreshResponse response =
                        mapper.readValue(record.value(), UserRecommendationRefreshResponse.class);
                databaseHandler.releaseClient(response.name());

                Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1, null)
                );
                consumer.commitSync(offsets);
            }
        }
    }

    public void distributeParseRequest(UserRecommendationRefreshRequest request) throws JsonProcessingException {
        final ObjectMapper mapper = new ObjectMapper();
        log.info("Refresh Request sent via kafka... user: {}", request.name());
        ProducerRecord<String, String> record = new ProducerRecord<>(RECOMMENDATION_REQUEST_TOPIC,
                mapper.writeValueAsString(request));
        topicProducer.send(record);
    }

}
