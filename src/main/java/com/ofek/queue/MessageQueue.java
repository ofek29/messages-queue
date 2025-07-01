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

public class MessageQueue {
    private final BlockingQueue<Message> queue = new LinkedBlockingQueue<>();
    private final BlockingQueue<Message> persistenceQueue = new LinkedBlockingQueue<>();
    private final Path messagesFilePath;
    private final ExecutorService persistenceExecutor;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    // Configuration
    private final int batchSize;
    private final boolean enableConsoleLogging;

    /**
     * Creates a MessageQueue with default settings (batch size 100, no console
     * logging)
     */
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
        this.batchSize = batchSize;
        this.enableConsoleLogging = enableConsoleLogging;
        this.persistenceExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "MessageQueue-Persistence");
            t.setDaemon(true);
            return t;
        });

        loadMessagesFromFile();
        startPersistenceWorker();
    }

    /**
     * Adds a message to the queue. This operation is non-blocking and thread-safe.
     * The message will be persisted to file asynchronously in batches.
     */
    public void enqueue(Message message) {
        lock.readLock().lock();
        try {
            queue.offer(message);
            persistenceQueue.offer(message); // Async persistence
            if (enableConsoleLogging) {
                System.out.println("Enqueued: " + message);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Removes and returns a message from the queue, or null if empty.
     * This operation is thread-safe and allows concurrent access.
     */
    public Message dequeue() {
        lock.readLock().lock();
        try {
            return queue.poll();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns the current number of messages in the queue.
     */
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
                    saveBatchToFile(batch);
                    batch.clear();

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    System.err.println("Error in persistence worker: " + e.getMessage());
                }
            }
        });
    }

    private void saveBatchToFile(List<Message> messages) {
        if (messages.isEmpty())
            return;

        try (BufferedWriter writer = Files.newBufferedWriter(
                messagesFilePath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            for (Message message : messages) {
                writer.write(message.getId() + "|" + message.getPayload());
                writer.newLine();
            }
            writer.flush(); // Ensure data is written
        } catch (IOException e) {
            System.err.println("Error saving messages to file: " + e.getMessage());
        }
    }

    private void loadMessagesFromFile() {
        if (!Files.exists(messagesFilePath)) {
            if (enableConsoleLogging) {
                System.out.println("File " + messagesFilePath + " doesn't exist. Starting with empty queue.");
            }
            return;
        }

        try (BufferedReader reader = Files.newBufferedReader(messagesFilePath)) {
            String line;
            int loadedCount = 0;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|", 2);
                if (parts.length == 2) {
                    String id = parts[0];
                    String payload = parts[1];
                    Message message = new Message(id, payload);
                    queue.offer(message);
                    loadedCount++;
                }
            }
            if (enableConsoleLogging) {
                System.out.println("Loaded " + loadedCount + " messages from file");
            }
        } catch (IOException e) {
            System.err.println("Error loading messages from file: " + e.getMessage());
        }
    }
}
