package com.ofek.queue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@DisplayName("Integration Tests")
public class IntegrationTest {

    @TempDir
    Path tempDir;

    private Path testFile;

    @BeforeEach
    void setUp() {
        testFile = tempDir.resolve("integration_test.log");
    }

    @Test
    @DisplayName("Should handle complete workflow: produce, consume, and persist")
    void testCompleteWorkflow() throws InterruptedException, IOException {
        int messageCount = 24;
        int batchSize = 6;

        // Create queue with specific batch size
        MessageQueue batchQueue = new MessageQueue(testFile.toString(), batchSize, false);
        Producer batchProducer = new Producer(batchQueue);
        Consumer batchConsumer = new Consumer(batchQueue);

        // Produce messages
        for (int i = 0; i < messageCount; i++) {
            batchProducer.produce("Workflow message " + i);
        }

        assertEquals(messageCount, batchQueue.size());

        // Consume half of the messages
        int consumeCount = messageCount / 2;
        for (int i = 0; i < consumeCount; i++) {
            Message message = batchConsumer.poll();
            assertNotNull(message);
            assertEquals("Workflow message " + i, message.getPayloadAsString());
        }

        assertEquals(messageCount - consumeCount, batchQueue.size());

        // Wait for persistence and verify file content
        Thread.sleep(200); // Allow persistence to occur

        // Shutdown to ensure all messages are persisted
        batchQueue.shutdown();
        Thread.sleep(200);

        // Verify file contains all produced messages
        assertTrue(Files.exists(testFile));
        List<String> lines = Files.readAllLines(testFile);
        assertEquals(messageCount, lines.size());

        // Verify recovery works
        MessageQueue recoveredQueue = new MessageQueue(testFile.toString(), batchSize, false);
        assertEquals(messageCount, recoveredQueue.size());

        // Verify messages are in correct order
        Consumer recoveredConsumer = new Consumer(recoveredQueue);
        for (int i = 0; i < messageCount; i++) {
            Message message = recoveredConsumer.poll();
            assertNotNull(message);
            assertEquals("Workflow message " + i, message.getPayloadAsString());
        }

        recoveredQueue.shutdown();
    }

    @Test
    @DisplayName("Should handle concurrent producers and consumers with persistence")
    void testConcurrentOperationsWithPersistence() throws InterruptedException, IOException {
        int numProducers = 4;
        int numConsumers = 2;
        int messagesPerProducer = 50;
        int batchSize = 10;

        MessageQueue concurrentQueue = new MessageQueue(testFile.toString(), batchSize, false);

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
                    Producer threadProducer = new Producer(concurrentQueue);
                    for (int j = 0; j < messagesPerProducer; j++) {
                        threadProducer.produce("Producer-" + producerId + "-Message-" + j);
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
                    Consumer threadConsumer = new Consumer(concurrentQueue);
                    while (producedCount.get() < numProducers * messagesPerProducer || concurrentQueue.size() > 0) {
                        Message message = threadConsumer.poll();
                        if (message != null) {
                            consumedCount.incrementAndGet();
                        }
                        Thread.yield();
                    }
                } finally {
                    consumerLatch.countDown();
                }
            });
        }

        // Wait for completion
        assertTrue(producerLatch.await(10, TimeUnit.SECONDS));
        assertTrue(consumerLatch.await(10, TimeUnit.SECONDS));

        executor.shutdown();

        // Verify all messages were processed
        assertEquals(numProducers * messagesPerProducer, producedCount.get());
        assertEquals(numProducers * messagesPerProducer, consumedCount.get());
        assertEquals(0, concurrentQueue.size());

        // Shutdown and verify persistence
        concurrentQueue.shutdown();
        Thread.sleep(200);

        assertTrue(Files.exists(testFile));
        List<String> lines = Files.readAllLines(testFile);
        assertEquals(numProducers * messagesPerProducer, lines.size());
    }

    @Test
    @DisplayName("Should handle system restart simulation")
    void testSystemRestartSimulation() throws InterruptedException, IOException {
        int messageCount = 100;
        int batchSize = 20;

        // Initial system operation
        MessageQueue initialQueue = new MessageQueue(testFile.toString(), batchSize, false);
        Producer initialProducer = new Producer(initialQueue);
        Consumer initialConsumer = new Consumer(initialQueue);

        // Produce messages
        for (int i = 0; i < messageCount; i++) {
            initialProducer.produce("Restart test message " + i);
        }

        // Consume some messages
        int consumedInPhase1 = 30;
        for (int i = 0; i < consumedInPhase1; i++) {
            Message message = initialConsumer.poll();
            assertNotNull(message);
            assertEquals("Restart test message " + i, message.getPayloadAsString());
        }

        // Simulate system shutdown
        initialQueue.shutdown();
        Thread.sleep(300);

        // System restart
        MessageQueue restartedQueue = new MessageQueue(testFile.toString(), batchSize, false);

        // Verify remaining messages are recovered
        int expectedRecoveredCount = messageCount; // All messages should be persisted
        assertEquals(expectedRecoveredCount, restartedQueue.size());

        // Continue consuming
        Consumer restartedConsumer = new Consumer(restartedQueue);
        for (int i = 0; i < messageCount; i++) {
            Message message = restartedConsumer.poll();
            assertNotNull(message);
            assertEquals("Restart test message " + i, message.getPayloadAsString());
        }

        assertEquals(0, restartedQueue.size());

        restartedQueue.shutdown();
    }

    @Test
    @DisplayName("Should handle varying batch sizes correctly")
    void testVaryingBatchSizes() throws InterruptedException, IOException {
        int[] batchSizes = { 1, 5, 10, 25, 50 };
        int messagesPerBatch = 100;

        for (int batchSize : batchSizes) {
            Path batchTestFile = tempDir.resolve("batch_test_" + batchSize + ".log");

            MessageQueue batchQueue = new MessageQueue(batchTestFile.toString(), batchSize, false);
            Producer batchProducer = new Producer(batchQueue);

            // Produce messages
            for (int i = 0; i < messagesPerBatch; i++) {
                batchProducer.produce("Batch-" + batchSize + "-Message-" + i);
            }

            // Shutdown to ensure persistence
            batchQueue.shutdown();
            Thread.sleep(200);

            // Verify file content
            assertTrue(Files.exists(batchTestFile));
            List<String> lines = Files.readAllLines(batchTestFile);
            assertEquals(messagesPerBatch, lines.size());

            // Verify recovery
            MessageQueue recoveredQueue = new MessageQueue(batchTestFile.toString(), batchSize, false);
            assertEquals(messagesPerBatch, recoveredQueue.size());

            recoveredQueue.shutdown();
        }
    }

    @Test
    @DisplayName("Should handle mixed message sizes")
    void testMixedMessageSizes() throws InterruptedException, IOException {
        MessageQueue mixedQueue = new MessageQueue(testFile.toString(), 10, false);
        Producer mixedProducer = new Producer(mixedQueue);
        Consumer mixedConsumer = new Consumer(mixedQueue);

        // Produce messages of varying sizes
        mixedProducer.produce("Short");
        mixedProducer.produce("Medium length message with some content");

        StringBuilder longMessage = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longMessage.append("Long message content ");
        }
        mixedProducer.produce(longMessage.toString());

        mixedProducer.produce(""); // Empty message
        mixedProducer.produce("Final message");

        assertEquals(5, mixedQueue.size());

        // Consume and verify
        Message msg1 = mixedConsumer.poll();
        assertEquals("Short", msg1.getPayloadAsString());

        Message msg2 = mixedConsumer.poll();
        assertEquals("Medium length message with some content", msg2.getPayloadAsString());

        Message msg3 = mixedConsumer.poll();
        assertEquals(longMessage.toString(), msg3.getPayloadAsString());

        Message msg4 = mixedConsumer.poll();
        assertEquals("", msg4.getPayloadAsString());

        Message msg5 = mixedConsumer.poll();
        assertEquals("Final message", msg5.getPayloadAsString());

        assertEquals(0, mixedQueue.size());

        // Shutdown and verify persistence
        mixedQueue.shutdown();
        Thread.sleep(200);

        assertTrue(Files.exists(testFile));
        List<String> lines = Files.readAllLines(testFile);
        assertEquals(5, lines.size());
    }

    @Test
    @DisplayName("Should handle rapid start-stop cycles")
    void testRapidStartStopCycles() throws InterruptedException, IOException {
        int cycles = 5;
        int messagesPerCycle = 20;

        for (int cycle = 0; cycle < cycles; cycle++) {
            MessageQueue cycleQueue = new MessageQueue(testFile.toString(), 10, false);
            Producer cycleProducer = new Producer(cycleQueue);

            // Produce messages
            for (int i = 0; i < messagesPerCycle; i++) {
                cycleProducer.produce("Cycle-" + cycle + "-Message-" + i);
            }

            // Quick shutdown
            cycleQueue.shutdown();
            Thread.sleep(100);
        }

        // Verify all messages were persisted
        assertTrue(Files.exists(testFile));
        List<String> lines = Files.readAllLines(testFile);
        assertEquals(cycles * messagesPerCycle, lines.size());

        // Verify recovery
        MessageQueue finalQueue = new MessageQueue(testFile.toString(), 10, false);
        assertEquals(cycles * messagesPerCycle, finalQueue.size());

        finalQueue.shutdown();
    }
}
