package com.ofek.queue;

import java.io.Serializable;
import java.util.UUID;

public class Message implements Serializable {
    private final String id;
    private final String payload;

    // Constructor for new messages (generates new ID)
    public Message(String payload) {
        this.id = UUID.randomUUID().toString();
        this.payload = payload;
    }

    // Constructor for loading existing messages (preserves original ID)
    public Message(String id, String payload) {
        this.id = id;
        this.payload = payload;
    }

    public String getId() {
        return id;
    }

    public String getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "Message{" +
                "id='" + id + '\'' +
                ", payload='" + payload + '\'' +
                '}';
    }
}