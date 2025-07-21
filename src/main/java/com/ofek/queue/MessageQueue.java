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
import java.util.Base64;

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
        this(filename, 100, false);
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
     * Shuts down the message queue, ensuring all messages are persisted.
     * Call this method before application shutdown.
     */
    public void shutdown() {
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
            List<Message> batch = new ArrayList<>(batchSize);
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    Message message = persistenceQueue.take(); // Blocking to wait for messages
                    batch.add(message);
                    persistenceQueue.drainTo(batch, batchSize - batch.size());

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
            for (Message message : messages) {
                // Encode byte array as Base64 for safe text storage
                String encodedPayload = Base64.getEncoder().encodeToString(message.getPayload());
                writer.write(encodedPayload);
                writer.newLine();
            }
            writer.flush(); // Ensure data is written
        } catch (IOException e) {
            System.err.println("Error saving messages to file: " + e.getMessage());
        } finally {
            messages.clear();
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
                String encodedPayload = line;
                // Decode Base64 back to byte array
                byte[] payload = Base64.getDecoder().decode(encodedPayload);
                Message message = new Message(payload);
                queue.offer(message);
                loadedCount++;
            }
            if (enableConsoleLogging) {
                System.out.println("Loaded " + loadedCount + " messages from file");
            }
        } catch (IOException e) {
            System.err.println("Error loading messages from file: " + e.getMessage());
        }
    }
}
