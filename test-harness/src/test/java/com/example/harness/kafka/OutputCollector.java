package com.example.harness.kafka;

import com.example.common.Message;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe collection of the messages the harness received from
 * topic c, keyed by correlation id.
 *
 * <p>The listener container thread writes; step-definition threads read
 * (via Awaitility). No manual thread joining.</p>
 */
public class OutputCollector {

    private final ConcurrentHashMap<String, Message> collected = new ConcurrentHashMap<>();

    public void add(Message message) {
        collected.put(message.getId(), message);
    }

    public Optional<Message> get(String id) {
        return Optional.ofNullable(collected.get(id));
    }

    public boolean contains(String id) {
        return collected.containsKey(id);
    }

    public void clear() {
        collected.clear();
    }
}
