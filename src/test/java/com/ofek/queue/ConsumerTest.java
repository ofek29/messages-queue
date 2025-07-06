package com.ofek.queue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;

@DisplayName("Consumer Tests")
public class ConsumerTest {

    @TempDir
    Path tempDir;

    private MessageQueue messageQueue;
    private Producer producer;
    private Consumer consumer;

    @BeforeEach
    void setUp() {
        Path testFile = tempDir.resolve("consumer_test.log");
        messageQueue = new MessageQueue(testFile.toString(), 10, false);
        producer = new Producer(messageQueue);
        consumer = new Consumer(messageQueue);
    }

    @AfterEach
    void tearDown() {
        if (messageQueue != null) {
            messageQueue.shutdown();
        }
    }

    @Test
    @DisplayName("Should consume single message")
    void testConsumeSingleMessage() {
        producer.produce("Test message");

        Message message = consumer.poll();
        assertNotNull(message);
        assertEquals("Test message", message.getPayload());
        assertEquals(0, messageQueue.size());
    }

    @Test
    @DisplayName("Should consume multiple messages in FIFO order")
    void testConsumeMultipleMessages() {
        int messageCount = 5;

        for (int i = 0; i < messageCount; i++) {
            producer.produce("Message " + i);
        }

        assertEquals(messageCount, messageQueue.size());

        for (int i = 0; i < messageCount; i++) {
            Message message = consumer.poll();
            assertNotNull(message);
            assertEquals("Message " + i, message.getPayload());
        }

        assertEquals(0, messageQueue.size());
    }

    @Test
    @DisplayName("Should return null when consuming from empty queue")
    void testConsumeFromEmptyQueue() {
        assertEquals(0, messageQueue.size());

        Message message = consumer.poll();
        assertNull(message);

        assertEquals(0, messageQueue.size());
    }

    @Test
    @DisplayName("Should handle repeated polling of empty queue")
    void testRepeatedPollingEmptyQueue() {
        assertEquals(0, messageQueue.size());

        for (int i = 0; i < 10; i++) {
            Message message = consumer.poll();
            assertNull(message);
        }

        assertEquals(0, messageQueue.size());
    }

    @Test
    @DisplayName("Should consume messages as they are produced")
    void testConsumeAsProduced() {
        // Initially empty
        assertEquals(0, messageQueue.size());
        assertNull(consumer.poll());

        // Produce and consume one by one
        producer.produce("Message 1");
        assertEquals(1, messageQueue.size());

        Message msg1 = consumer.poll();
        assertNotNull(msg1);
        assertEquals("Message 1", msg1.getPayload());
        assertEquals(0, messageQueue.size());

        producer.produce("Message 2");
        assertEquals(1, messageQueue.size());

        Message msg2 = consumer.poll();
        assertNotNull(msg2);
        assertEquals("Message 2", msg2.getPayload());
        assertEquals(0, messageQueue.size());
    }

    @Test
    @DisplayName("Should handle large messages")
    void testConsumeLargeMessage() {
        StringBuilder largePayload = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            largePayload.append("Large payload content ");
        }

        producer.produce(largePayload.toString());

        Message message = consumer.poll();
        assertNotNull(message);
        assertEquals(largePayload.toString(), message.getPayload());
        assertEquals(0, messageQueue.size());
    }

    @Test
    @DisplayName("Should handle empty payload messages")
    void testConsumeEmptyPayload() {
        producer.produce("");

        Message message = consumer.poll();
        assertNotNull(message);
        assertEquals("", message.getPayload());
        assertEquals(0, messageQueue.size());
    }

    @Test
    @DisplayName("Should handle partial queue consumption")
    void testPartialConsumption() {
        int totalMessages = 10;
        int consumeCount = 5;

        // Produce all messages
        for (int i = 0; i < totalMessages; i++) {
            producer.produce("Message " + i);
        }

        assertEquals(totalMessages, messageQueue.size());

        // Consume only part of the messages
        for (int i = 0; i < consumeCount; i++) {
            Message message = consumer.poll();
            assertNotNull(message);
            assertEquals("Message " + i, message.getPayload());
        }

        assertEquals(totalMessages - consumeCount, messageQueue.size());

        // Verify remaining messages are still in correct order
        for (int i = consumeCount; i < totalMessages; i++) {
            Message message = consumer.poll();
            assertNotNull(message);
            assertEquals("Message " + i, message.getPayload());
        }

        assertEquals(0, messageQueue.size());
    }

    @Test
    @DisplayName("Should handle special characters in consumed messages")
    void testSpecialCharactersInConsumption() {
        String specialPayload = "Special chars: !@#$%^&*()_+[]{}|;:,.<>?`~";
        producer.produce(specialPayload);

        Message message = consumer.poll();
        assertNotNull(message);
        assertEquals(specialPayload, message.getPayload());
    }
}
