package com.ofek.queue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class PerformanceTest {

    private static final String PERFORMANCE_REPORT_FILE = "logs/performance_report.log";
    private static final int[] MESSAGE_COUNTS = { 1000, 10000, 100000, 1000000 };

    // Intelligent thread count based on system capabilities
    private static final int CPU_CORES = Runtime.getRuntime().availableProcessors();
    private static final int THREAD_COUNT = CPU_CORES * 2;

    private static final String PAYLOAD = generatePayload(100);

    public static void main(String[] args) {
        PerformanceTest test = new PerformanceTest();
        try {
            test.runAllTests();
        } catch (Exception e) {
            System.err.println("Error running performance tests: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void runAllTests() throws IOException {
        System.out.println("Starting Performance Tests...");
        System.out.println("=================================");
        System.out.println("System Info:");
        System.out.println("  CPU Cores: " + CPU_CORES);
        System.out.println("  Thread Count: " + THREAD_COUNT);
        System.out.println("=================================");

        // Initialize report file
        initializeReportFile();

        // Run single-threaded tests
        for (int messageCount : MESSAGE_COUNTS) {
            System.out.println("\nTesting with " + messageCount + " messages (Single-threaded):");
            runSingleThreadedTest(messageCount);
        }

        // Run multi-threaded tests
        for (int messageCount : MESSAGE_COUNTS) {
            System.out.println(
                    "\nTesting with " + messageCount + " messages (Multi-threaded, " + THREAD_COUNT + " threads):");
            runMultiThreadedTest(messageCount);
        }

        System.out.println("\nPerformance tests completed. Results saved to: " + PERFORMANCE_REPORT_FILE);
    }

    private void runSingleThreadedTest(int messageCount) throws IOException {
        // Clean up any existing messages file
        cleanupMessagesFile();

        MessageQueue queue = new MessageQueue("logs/messages_perf_test.log");
        Producer producer = new Producer(queue);
        Consumer consumer = new Consumer(queue);

        // Test enqueue performance
        long startTime = System.nanoTime();
        for (int i = 0; i < messageCount; i++) {
            producer.produce(PAYLOAD);
        }
        long enqueueTime = System.nanoTime() - startTime;

        // Test dequeue performance
        startTime = System.nanoTime();
        int processedMessages = 0;
        while (queue.size() > 0) {
            Message message = consumer.poll();
            if (message != null) {
                processedMessages++;
            }
        }
        long dequeueTime = System.nanoTime() - startTime;

        // Calculate metrics
        double enqueueTimeMs = enqueueTime / 1_000_000.0;
        double dequeueTimeMs = dequeueTime / 1_000_000.0;
        double totalTimeMs = enqueueTimeMs + dequeueTimeMs;
        double throughputMsg = messageCount / (totalTimeMs / 1000.0);

        // Display results
        String results = String.format(
                "Messages: %,d | Enqueue: %.2f ms | Dequeue: %.2f ms | Total: %.2f ms | Throughput: %.2f msg/sec | Processed: %d",
                messageCount, enqueueTimeMs, dequeueTimeMs, totalTimeMs, throughputMsg, processedMessages);
        System.out.println(results);

        // Save to report
        saveToReport("SINGLE-THREADED TEST", messageCount, results, enqueueTimeMs, dequeueTimeMs, totalTimeMs,
                throughputMsg);

        // Clean up
        queue.shutdown();
        cleanupMessagesFile();
    }

    private void runMultiThreadedTest(int messageCount) throws IOException {
        // Clean up any existing messages file
        cleanupMessagesFile();

        MessageQueue queue = new MessageQueue("logs/messages_perf_test.log");
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT * 2); // Producers + Consumers

        int messagesPerThread = messageCount / THREAD_COUNT;
        List<Future<?>> futures = new ArrayList<>();

        long startTime = System.nanoTime();

        // Start producer threads
        for (int t = 0; t < THREAD_COUNT; t++) {
            Future<?> future = executor.submit(() -> {
                Producer producer = new Producer(queue);
                for (int i = 0; i < messagesPerThread; i++) {
                    producer.produce(PAYLOAD);
                }
            });
            futures.add(future);
        }

        // Wait for all producers to finish
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                System.err.println("Producer thread error: " + e.getMessage());
            }
        }

        long enqueueTime = System.nanoTime() - startTime;

        // Start consumer threads
        futures.clear();
        startTime = System.nanoTime();

        for (int t = 0; t < THREAD_COUNT; t++) {
            Future<?> future = executor.submit(() -> {
                Consumer consumer = new Consumer(queue);
                while (queue.size() > 0) {
                    Message message = consumer.poll();
                    if (message == null) {
                        break; // No more messages
                    }
                }
            });
            futures.add(future);
        }

        // Wait for all consumers to finish
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                System.err.println("Consumer thread error: " + e.getMessage());
            }
        }

        long dequeueTime = System.nanoTime() - startTime;

        queue.shutdown();
        executor.shutdown();
        try {
            executor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Calculate metrics
        double enqueueTimeMs = enqueueTime / 1_000_000.0;
        double dequeueTimeMs = dequeueTime / 1_000_000.0;
        double totalTimeMs = enqueueTimeMs + dequeueTimeMs;
        double throughputMsg = messageCount / (totalTimeMs / 1000.0);

        // Display results
        String results = String.format(
                "Messages: %,d | Threads: %d | Enqueue: %.2f ms | Dequeue: %.2f ms | Total: %.2f ms | Throughput: %.2f msg/sec",
                messageCount, THREAD_COUNT, enqueueTimeMs, dequeueTimeMs, totalTimeMs, throughputMsg);
        System.out.println(results);

        // Save to report
        saveToReport("MULTI-THREADED TEST (" + THREAD_COUNT + " threads)", messageCount, results, enqueueTimeMs,
                dequeueTimeMs, totalTimeMs, throughputMsg);

        // Clean up
        cleanupMessagesFile();
    }

    private static String generatePayload(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) ('A' + (i % 26)));
        }
        return sb.toString();
    }

    private void cleanupMessagesFile() {
        try {
            Files.deleteIfExists(Path.of("logs/messages_perf_test.log"));
        } catch (IOException e) {
            System.err.println("Error cleaning up messages file: " + e.getMessage());
        }
    }

    private void initializeReportFile() throws IOException {
        Path reportPath = Path.of(PERFORMANCE_REPORT_FILE);

        String header = String.format(
                "MESSAGE QUEUE PERFORMANCE TEST REPORT\n" +
                        "=====================================\n" +
                        "Test Date: %s\n" +
                        "Java Version: %s\n" +
                        "OS: %s %s\n" +
                        "Available Processors: %d\n" +
                        "Max Memory: %,d MB\n\n",
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                System.getProperty("java.version"),
                System.getProperty("os.name"),
                System.getProperty("os.version"),
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().maxMemory() / (1024 * 1024));

        Files.write(reportPath, header.getBytes(),
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private void saveToReport(String testType, int messageCount, String results,
            double enqueueTimeMs, double dequeueTimeMs,
            double totalTimeMs, double throughputMsg) throws IOException {
        Path reportPath = Path.of(PERFORMANCE_REPORT_FILE);

        String report = String.format(
                "%s\n" +
                        "Message Count: %,d\n" +
                        "Results: %s\n" +
                        "Detailed Metrics:\n" +
                        "  - Enqueue Time: %.2f ms (%.2f msg/sec)\n" +
                        "  - Dequeue Time: %.2f ms (%.2f msg/sec)\n" +
                        "  - Total Time: %.2f ms\n" +
                        "  - Overall Throughput: %.2f msg/sec\n" +
                        "  - Memory Usage: %,d MB\n\n",
                testType,
                messageCount,
                results,
                enqueueTimeMs, messageCount / (enqueueTimeMs / 1000.0),
                dequeueTimeMs, messageCount / (dequeueTimeMs / 1000.0),
                totalTimeMs,
                throughputMsg,
                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024));

        Files.write(reportPath, report.getBytes(), StandardOpenOption.APPEND);
    }
}
