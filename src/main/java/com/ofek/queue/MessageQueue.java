package com.ofek.queue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class MessageQueue {
    private final BlockingQueue<Message> queue = new LinkedBlockingQueue<>();
    private final Path messagesFilePath;

    public MessageQueue(String filename) {
        this(filename, 100, false); // Default: batch size 100, no console logging
    }

    /**
     * Creates a MessageQueue with custom configuration
     * 
     * @param filename             The file to persist messages to
     * @param batchSize            Number of messages to batch before writing to
     *                             file
     * @param enableConsoleLogging Whether to log enqueue operations to console
     */
    public MessageQueue(String filename, int batchSize, boolean enableConsoleLogging) {
        this.messagesFilePath = Path.of(filename);
        loadMessagesFromFile();
    }

    public void enqueue(Message message) {
        queue.offer(message);
        saveMessageToFile(message);
        System.out.println("Enqueued: " + message);
    }

    public Message dequeue() {
        return queue.poll();
    }

    public int size() {
        lock.readLock().lock();
        try {
            return queue.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Forces all pending messages to be written to disk.
     * This method blocks until all messages are persisted.
     */
    public void flush() {
        // Force persistence of all pending messages
        synchronized (persistenceQueue) {
            while (!persistenceQueue.isEmpty()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    /**
     * Shuts down the message queue, ensuring all messages are persisted.
     * Call this method before application shutdown.
     */
    public void shutdown() {
        flush();
        persistenceExecutor.shutdown();
        try {
            // Wait for the persistence worker to finish
            if (!persistenceExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                persistenceExecutor.shutdownNow();
                // Wait a bit more for tasks to respond to being cancelled
                if (!persistenceExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                    System.err.println("Persistence worker did not terminate gracefully");
                }
            }
        } catch (InterruptedException ie) {
            // Re-interrupt the thread if we were interrupted while waiting
            persistenceExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void startPersistenceWorker() {
        persistenceExecutor.submit(() -> {
            List<Message> batch = new ArrayList<>();

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    // Collect messages in batches
                    Message message = persistenceQueue.take(); // Blocking
                    batch.add(message);

                    // Collect more messages if available (up to batch size)
                    while (batch.size() < batchSize && !persistenceQueue.isEmpty()) {
                        Message additional = persistenceQueue.poll();
                        if (additional != null) {
                            batch.add(additional);
                        }
                    }

                    // Write batch to file
                    if (batch.size() >= batchSize) {
                        saveBatchToFile(batch);
                    }
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.err.println("Error in persistence worker: " + e.getMessage());
            } finally {
                // Ensure any remaining messages are saved
                if (!batch.isEmpty()) {
                    saveBatchToFile(batch);
                }
                if (!persistenceQueue.isEmpty()) {
                    List<Message> remainingMessages = new ArrayList<>();
                    persistenceQueue.drainTo(remainingMessages);
                    saveBatchToFile(remainingMessages);
                }
            }
        });
    }

    private void saveBatchToFile(List<Message> messages) {
        if (messages.isEmpty())
            return;

        try (BufferedWriter writer = Files.newBufferedWriter(
                messagesFilePath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            writer.write(message.getId() + "|" + message.getPayload());
            writer.newLine();
        } catch (IOException e) {
            System.err.println("Error saving messages to file: " + e.getMessage());
        }
    }

    private void loadMessagesFromFile() {
        if (!Files.exists(messagesFilePath)) {
            System.out.println("File " + messagesFilePath + " doesn't exist. Starting with empty queue.");
            return;
        }

        try (BufferedReader reader = Files.newBufferedReader(messagesFilePath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|", 2);
                if (parts.length == 2) {
                    String id = parts[0];
                    String payload = parts[1];
                    Message message = new Message(id, payload);
                    queue.offer(message);
                }
            }
        } catch (IOException e) {
            System.out.println("Error loading messages from file: " + e.getMessage());
        }

    }

}
