package com.mathsena.orderservice.kafka;

import com.mathsena.basedomains.dto.OrderEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

@Service
public class OrderProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderProducer.class);

    private final NewTopic topic;
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public OrderProducer(NewTopic topic, KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(OrderEvent orderEvent) {
        LOGGER.info("Preparing to send order event: {}", orderEvent);

        Message<OrderEvent> message = MessageBuilder
                .withPayload(orderEvent)
                .setHeader(KafkaHeaders.TOPIC, topic.name())
                .build();

        try {
            // Block until the message is sent and get the result
            SendResult<String, OrderEvent> result = kafkaTemplate.send(message).get();

            // Log successful sending
            LOGGER.info("Successfully sent message=[{}] with offset=[{}]", orderEvent, result.getRecordMetadata().offset());
        } catch (InterruptedException e) {
            // Handle the interruption
            Thread.currentThread().interrupt();
            LOGGER.error("Interrupted while sending message=[{}]: {}", orderEvent, e.getMessage());
        } catch (ExecutionException e) {
            // Handle the underlying error
            LOGGER.error("An error occurred while sending message=[{}]: {}", orderEvent, e.getMessage());
        }
    }
}
