package com.ofek.queue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;

@DisplayName("Producer Tests")
public class ProducerTest {

    @TempDir
    Path tempDir;

    private MessageQueue messageQueue;
    private Producer producer;

    @BeforeEach
    void setUp() {
        Path testFile = tempDir.resolve("producer_test.log");
        messageQueue = new MessageQueue(testFile.toString(), 10, false);
        producer = new Producer(messageQueue);
    }

    @AfterEach
    void tearDown() {
        if (messageQueue != null) {
            messageQueue.shutdown();
        }
    }

    @Test
    @DisplayName("Should produce single message")
    void testProduceSingleMessage() {
        producer.produce("Test message");

        assertEquals(1, messageQueue.size());

        Message message = messageQueue.dequeue();
        assertNotNull(message);
        assertEquals("Test message", message.getPayloadAsString());
    }

    @Test
    @DisplayName("Should produce multiple messages")
    void testProduceMultipleMessages() {
        int messageCount = 5;

        for (int i = 0; i < messageCount; i++) {
            producer.produce("Message " + i);
        }

        assertEquals(messageCount, messageQueue.size());

        for (int i = 0; i < messageCount; i++) {
            Message message = messageQueue.dequeue();
            assertNotNull(message);
            assertEquals("Message " + i, message.getPayloadAsString());
        }
    }

    @Test
    @DisplayName("Should handle empty payload")
    void testProduceEmptyPayload() {
        producer.produce("");

        assertEquals(1, messageQueue.size());

        Message message = messageQueue.dequeue();
        assertNotNull(message);
        assertEquals("", message.getPayloadAsString());
    }

    @Test
    @DisplayName("Should handle large payload")
    void testProduceLargePayload() {
        StringBuilder largePayload = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            largePayload.append("Large payload content ");
        }

        producer.produce(largePayload.toString());

        assertEquals(1, messageQueue.size());

        Message message = messageQueue.dequeue();
        assertNotNull(message);
        assertEquals(largePayload.toString(), message.getPayloadAsString());
    }

    @Test
    @DisplayName("Should handle special characters in payload")
    void testSpecialCharactersInPayload() {
        String specialPayload = "Special chars: !@#$%^&*()_+[]{}|;:,.<>?`~";
        producer.produce(specialPayload);

        assertEquals(1, messageQueue.size());

        Message message = messageQueue.dequeue();
        assertNotNull(message);
        assertEquals(specialPayload, message.getPayloadAsString());
    }
}
