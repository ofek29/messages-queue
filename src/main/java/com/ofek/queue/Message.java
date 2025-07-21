package com.ofek.queue;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

public class Message implements Serializable {
    private final byte[] payload;

    // Constructor for new messages
    public Message(byte[] payload) {
        this.payload = payload.clone();
    }

    // Constructor for String payload
    public Message(String payload) {
        this.payload = payload.getBytes(StandardCharsets.UTF_8);
    }

    public byte[] getPayload() {
        return payload.clone();
    }

    // Get payload as String
    public String getPayloadAsString() {
        return new String(payload, StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        return "Message{" +
                ", payload='" + getPayloadAsString() + '\'' +
                '}';
    }
}