package com.example.app2;

import com.example.common.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Consumes {@code topic b}, adds the configured constant to the value and
 * emits the result (same correlation id, id used as the Kafka key) to
 * {@code topic c}.
 */
@Component
public class App2Listener {

    private static final Logger log = LoggerFactory.getLogger(App2Listener.class);

    private final App2Properties properties;
    private final KafkaTemplate<String, Message> kafkaTemplate;

    public App2Listener(App2Properties properties, KafkaTemplate<String, Message> kafkaTemplate) {
        this.properties = properties;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "${app2.input-topic:b}", groupId = "${spring.kafka.consumer.group-id:app2}")
    public void onMessage(Message message) {
        long result = message.getValue() + properties.getConstant();
        Message out = new Message(message.getId(), result);
        // key = correlation id => per-key ordering end to end
        kafkaTemplate.send(properties.getOutputTopic(), out.getId(), out);
        log.info("[App2] id={} value={} + constant={} = {} -> published to topic '{}'",
                message.getId(), message.getValue(), properties.getConstant(), result,
                properties.getOutputTopic());
    }
}
