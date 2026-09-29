package com.example.common;

import java.util.Objects;

/**
 * The message contract shared by both apps and the test harness.
 *
 * <p>{@code id} is the correlation id and is also used as the Kafka key
 * end-to-end (publish -> topic a -> App1 -> topic b -> App2 -> topic c),
 * which guarantees per-key ordering through the whole chain.</p>
 */
public class Message {

    private String id;   // correlation id (also the Kafka record key)
    private long value;

    public Message() {
    }

    public Message(String id, long value) {
        this.id = id;
        this.value = value;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Message message = (Message) o;
        return Objects.equals(id, message.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "Message{id='" + id + "', value=" + value + '}';
    }
}
