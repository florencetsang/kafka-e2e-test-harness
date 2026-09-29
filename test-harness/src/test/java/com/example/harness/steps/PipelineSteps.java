package com.example.harness.steps;

import com.example.common.Message;
import com.example.harness.kafka.OutputCollector;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Step definitions for pipeline.feature.
 *
 * <p>Publishes to topic a with the correlation id as the Kafka key, then
 * asserts asynchronously (Awaitility, matched by id) that the transformed
 * message arrived on topic c.</p>
 */
public class PipelineSteps {

    private static final Logger log = LoggerFactory.getLogger(PipelineSteps.class);

    private static final long DEFAULT_TIMEOUT_SECONDS = 15;

    private final KafkaTemplate<String, Message> kafkaTemplate;
    private final OutputCollector collector;

    /** Timeout captured by the When step, used by the Then assertion. */
    private long timeoutSeconds = DEFAULT_TIMEOUT_SECONDS;

    public PipelineSteps(KafkaTemplate<String, Message> kafkaTemplate, OutputCollector collector) {
        this.kafkaTemplate = kafkaTemplate;
        this.collector = collector;
    }

    @Given("a message with id {string} and value {long} is published to topic {string}")
    public void publishMessage(String id, long value, String topic) {
        Message message = new Message(id, value);
        kafkaTemplate.send(topic, id, message);
        log.info("[Harness] published {} to topic '{}'", message, topic);
    }

    @When("the pipeline processes it within {long} seconds")
    public void pipelineProcessesItWithin(long seconds) {
        this.timeoutSeconds = seconds;
        log.info("[Harness] pipeline expected to process the message within {} seconds", seconds);
    }

    @Then("topic {string} should contain a message with id {string} and value {long}")
    public void assertTopicContains(String topic, String id, long expected) {
        log.info("[Harness] awaiting id='{}' on topic '{}' with expected value {}", id, topic, expected);
        Awaitility.await()
                .atMost(Duration.ofSeconds(timeoutSeconds))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    Message message = collector.get(id)
                            .orElseThrow(() -> new AssertionError("no message for id " + id));
                    assertThat(message.getValue()).isEqualTo(expected);
                });
        log.info("[Harness] asserted id='{}' on topic '{}' has value {}", id, topic, expected);
    }
}
