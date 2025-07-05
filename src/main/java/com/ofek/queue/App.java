package com.ofek.queue;

public class App {
    public static void main(String[] args) {
        MessageQueue queue = new MessageQueue("logs/messages.log", 100, false);

        // Create producer and consumer instances
        Producer producer = new Producer(queue);
        Consumer consumer = new Consumer(queue);

        System.out.println("Starting message production...");
        for (int i = 1; i <= 502; i++) {
            producer.produce("Hello, World!" + i);
        }

        System.out.println("Queue size: " + queue.size());

        // Loop to poll all messages
        System.out.println("Polling messages...");
        while (queue.size() > 0) {
            consumer.poll();
        }

        System.out.println("Queue size after pooling: " + queue.size());

        queue.shutdown();
        System.out.println("Queue shutdown complete.");
    }
}
