package com.example.app1;

import com.example.common.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Consumes {@code topic a}, multiplies the value by the configured
 * multiplier and emits the result (same correlation id, id used as the
 * Kafka key) to {@code topic b}.
 */
@Component
public class App1Listener {

    private static final Logger log = LoggerFactory.getLogger(App1Listener.class);

    private final App1Properties properties;
    private final KafkaTemplate<String, Message> kafkaTemplate;

    public App1Listener(App1Properties properties, KafkaTemplate<String, Message> kafkaTemplate) {
        this.properties = properties;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "${app1.input-topic:a}", groupId = "${spring.kafka.consumer.group-id:app1}")
    public void onMessage(Message message) {
        long result = message.getValue() * properties.getMultiplier();
        Message out = new Message(message.getId(), result);
        // key = correlation id => per-key ordering end to end
        kafkaTemplate.send(properties.getOutputTopic(), out.getId(), out);
        log.info("[App1] id={} value={} x multiplier={} = {} -> published to topic '{}'",
                message.getId(), message.getValue(), properties.getMultiplier(), result,
                properties.getOutputTopic());
    }
}
