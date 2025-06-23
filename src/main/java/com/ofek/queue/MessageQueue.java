package com.ofek.queue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class MessageQueue {
    private final BlockingQueue<Message> queue = new LinkedBlockingQueue<>();
    private final Path messagesFilePath;

    public MessageQueue(String filename) {
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
        return queue.size();
    }

    private void saveMessageToFile(Message message) {
        try (BufferedWriter writer = Files.newBufferedWriter(
                messagesFilePath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            writer.write(message.getId() + "|" + message.getPayload());
            writer.newLine();
        } catch (IOException e) {
            System.out.println("Error saving message to file: " + e.getMessage());
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
