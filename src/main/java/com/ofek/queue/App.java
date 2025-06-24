package com.ofek.queue;

public class App {
    public static void main(String[] args) {
        MessageQueue queue = new MessageQueue("messages.log");

        // Create producer and consumer instances
        Producer producer = new Producer(queue);
        Consumer consumer = new Consumer(queue);

        producer.produce("Hello, World!");
        producer.produce("Second message");
        producer.produce("Third message");

        // Loop to poll all messages
        System.out.println("Polling messages:");
        while (queue.size() > 0) {
            Message delivered = consumer.poll();
            System.out.println("Delivered: " + delivered);
        }

        System.out.println("Queue size: " + queue.size());
    }
}
