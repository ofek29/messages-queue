package com.ofek.queue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@DisplayName("MessageQueue Tests")
public class MessageQueueTest {

    @TempDir
    Path tempDir;

    private MessageQueue messageQueue;
    private Producer producer;
    private Consumer consumer;
    private Path testFile;

    @BeforeEach
    void setUp() {
        testFile = tempDir.resolve("message_queue_test.log");
        messageQueue = new MessageQueue(testFile.toString(), 5, false);
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
    @DisplayName("Should enqueue and dequeue messages in FIFO order")
    void testBasicEnqueueDequeue() {
        // Enqueue messages
        producer.produce("First message");
        producer.produce("Second message");
        producer.produce("Third message");

        assertEquals(3, messageQueue.size());

        // Dequeue messages and verify order
        Message first = consumer.poll();
        Message second = consumer.poll();
        Message third = consumer.poll();

        assertNotNull(first);
        assertNotNull(second);
        assertNotNull(third);

        assertEquals("First message", first.getPayloadAsString());
        assertEquals("Second message", second.getPayloadAsString());
        assertEquals("Third message", third.getPayloadAsString());

        assertEquals(0, messageQueue.size());
    }

    @Test
    @DisplayName("Should handle empty queue gracefully")
    void testEmptyQueueHandling() {
        assertEquals(0, messageQueue.size());

        Message message = consumer.poll();
        assertNull(message);

        assertEquals(0, messageQueue.size());
    }

    @Test
    @DisplayName("Should handle random number of messages")
    void testRandomNumberOfMessages() throws InterruptedException {
        Random random = new Random();
        int messageCount = random.nextInt(1000) + 1; // 1 to 1000 messages

        // Enqueue random number of messages
        for (int i = 0; i < messageCount; i++) {
            producer.produce("Random message " + i);
        }

        assertEquals(messageCount, messageQueue.size());

        // Dequeue all messages
        int dequeuedCount = 0;
        while (messageQueue.size() > 0) {
            Message message = consumer.poll();
            if (message != null) {
                dequeuedCount++;
                assertTrue(message.getPayloadAsString().startsWith("Random message"));
            }
        }

        assertEquals(messageCount, dequeuedCount);
        assertEquals(0, messageQueue.size());
    }

    @Test
    @DisplayName("Should persist messages when batch size is reached")
    void testBatchSizePersistence() throws InterruptedException, IOException {
        int batchSize = 3;
        MessageQueue batchQueue = new MessageQueue(testFile.toString(), batchSize,
                false);
        Producer batchProducer = new Producer(batchQueue);

        // Send exactly batch size messages
        for (int i = 0; i < batchSize; i++) {
            batchProducer.produce("Batch message " + i);
        }

        // Wait for persistence
        Thread.sleep(100);

        // Verify messages are saved to file
        assertTrue(Files.exists(testFile));
        List<String> lines = Files.readAllLines(testFile);
        assertEquals(batchSize, lines.size());

        // Verify file format
        for (int i = 0; i < batchSize; i++) {
            String decodedMessage = new String(Base64.getDecoder().decode(lines.get(i)));
            assertTrue(decodedMessage.contains("Batch message " + i));
        }

        batchQueue.shutdown();
    }

    @Test
    @DisplayName("Should handle messages less than batch size")
    void testLessThanBatchSize() throws InterruptedException, IOException {
        int batchSize = 10;
        MessageQueue batchQueue = new MessageQueue(testFile.toString(), batchSize,
                false);
        Producer batchProducer = new Producer(batchQueue);

        // Send fewer messages than batch size
        int messageCount = 3;
        for (int i = 0; i < messageCount; i++) {
            batchProducer.produce("Small batch message " + i);
        }

        // Shutdown to force persistence of remaining messages
        batchQueue.shutdown();

        // Wait for shutdown to complete
        Thread.sleep(200);

        // Verify messages are saved to file
        assertTrue(Files.exists(testFile));
        List<String> lines = Files.readAllLines(testFile);
        assertEquals(messageCount, lines.size());

        for (int i = 0; i < messageCount; i++) {
            String line = lines.get(i);
            String decodedMessage = new String(Base64.getDecoder().decode(line));
            assertTrue(decodedMessage.contains("Small batch message " + i));
        }
    }

    @Test
    @DisplayName("Should handle messages greater than batch size")
    void testGreaterThanBatchSize() throws InterruptedException, IOException {
        int batchSize = 3;
        MessageQueue batchQueue = new MessageQueue(testFile.toString(), batchSize, false);
        Producer batchProducer = new Producer(batchQueue);

        // Send more messages than batch size
        int messageCount = 10;
        for (int i = 0; i < messageCount; i++) {
            batchProducer.produce("Large batch message " + i);
        }

        // Shutdown to persist remaining messages
        batchQueue.shutdown();
        Thread.sleep(100);

        // Verify all messages are saved to file
        assertTrue(Files.exists(testFile));

        List<String> lines = Files.readAllLines(testFile);
        assertEquals(messageCount, lines.size());

        for (int i = 0; i < messageCount; i++) {
            String line = lines.get(i);
            String decodedMessage = new String(Base64.getDecoder().decode(line));
            assertTrue(decodedMessage.contains("Large batch message " + i));
        }
    }

    @Test
    @DisplayName("Should handle messages equal to batch size")
    void testEqualToBatchSize() throws InterruptedException, IOException {
        int batchSize = 5;
        MessageQueue batchQueue = new MessageQueue(testFile.toString(), batchSize,
                false);
        Producer batchProducer = new Producer(batchQueue);

        // Send exactly batch size messages
        for (int i = 0; i < batchSize; i++) {
            batchProducer.produce("Equal batch message " + i);
        }

        // Wait for persistence
        Thread.sleep(100);

        // Verify messages are saved to file
        assertTrue(Files.exists(testFile));
        List<String> lines = Files.readAllLines(testFile);
        assertEquals(batchSize, lines.size());

        batchQueue.shutdown();
    }

    @Test
    @DisplayName("Should recover messages from file on startup")
    void testMessageRecovery() throws InterruptedException, IOException {
        // Create and populate queue
        MessageQueue originalQueue = new MessageQueue(testFile.toString(), 3, false);
        Producer originalProducer = new Producer(originalQueue);

        for (int i = 0; i < 5; i++) {
            originalProducer.produce("Recovery message " + i);
        }

        // Shutdown to ensure persistence
        originalQueue.shutdown();
        Thread.sleep(100);

        // Create new queue with same file - should recover messages
        MessageQueue recoveredQueue = new MessageQueue(testFile.toString(), 3,
                false);

        // Verify recovered messages
        assertEquals(5, recoveredQueue.size());

        Consumer recoveredConsumer = new Consumer(recoveredQueue);
        for (int i = 0; i < 5; i++) {
            Message message = recoveredConsumer.poll();
            assertNotNull(message);
            assertEquals("Recovery message " + i, message.getPayloadAsString());
        }

        recoveredQueue.shutdown();
    }

    @Test
    @DisplayName("Should handle concurrent producers and consumers")
    void testConcurrentAccess() throws InterruptedException {
        int numProducers = 5;
        int numConsumers = 3;
        int messagesPerProducer = 100;

        ExecutorService executor = Executors.newFixedThreadPool(numProducers + numConsumers);
        CountDownLatch producerLatch = new CountDownLatch(numProducers);
        CountDownLatch consumerLatch = new CountDownLatch(numConsumers);

        AtomicInteger producedCount = new AtomicInteger(0);
        AtomicInteger consumedCount = new AtomicInteger(0);

        // Start producers
        for (int i = 0; i < numProducers; i++) {
            final int producerId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < messagesPerProducer; j++) {
                        producer.produce("Producer-" + producerId + "-Message-" + j);
                        producedCount.incrementAndGet();
                    }
                } finally {
                    producerLatch.countDown();
                }
            });
        }

        // Start consumers
        for (int i = 0; i < numConsumers; i++) {
            executor.submit(() -> {
                try {
                    while (producedCount.get() < numProducers * messagesPerProducer ||
                            messageQueue.size() > 0) {
                        Message message = consumer.poll();
                        if (message != null) {
                            consumedCount.incrementAndGet();
                        }
                    }
                } finally {
                    consumerLatch.countDown();
                }
            });
        }

        // Wait for completion
        assertTrue(producerLatch.await(5, TimeUnit.SECONDS));
        assertTrue(consumerLatch.await(5, TimeUnit.SECONDS));

        executor.shutdown();

        // Verify all messages were processed
        assertEquals(numProducers * messagesPerProducer, producedCount.get());
        assertEquals(numProducers * messagesPerProducer, consumedCount.get());
        assertEquals(0, messageQueue.size());
    }

    @Test
    @DisplayName("Should handle large messages")
    void testLargeMessages() throws InterruptedException {
        // Create large payload (1MB)
        StringBuilder largePayload = new StringBuilder();
        for (int i = 0; i < 1024 * 1024; i++) {
            largePayload.append("A");
        }

        producer.produce(largePayload.toString());
        assertEquals(1, messageQueue.size());

        Message message = consumer.poll();
        assertNotNull(message);
        assertEquals(largePayload.toString(), message.getPayloadAsString());
        assertEquals(0, messageQueue.size());
    }

    @Test
    @DisplayName("Should handle queue operations during shutdown")
    void testOperationsDuringShutdown() throws InterruptedException {
        // Add some messages
        producer.produce("Before shutdown");
        assertEquals(1, messageQueue.size());

        // Shutdown queue
        messageQueue.shutdown();

        // Operations should still work until the queue is fully shut down
        Message message = consumer.poll();
        assertNotNull(message);
        assertEquals("Before shutdown", message.getPayloadAsString());

        // check if message saved to file
        assertTrue(Files.exists(testFile));
        try {
            List<String> lines = Files.readAllLines(testFile);
            assertEquals(1, lines.size());
            String decodedMessage = new String(Base64.getDecoder().decode(lines.get(0)));
            assertTrue(decodedMessage.contains("Before shutdown"));
        } catch (IOException e) {
            fail("Failed to read from file after shutdown: " + e.getMessage());
        }
    }
}
