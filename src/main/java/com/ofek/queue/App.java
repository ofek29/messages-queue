package com.ofek.queue;

public class App {
    public static void main(String[] args) {
        MessageQueue queue = new MessageQueue("messages.log");

        queue.enqueue(new Message("Hello, World!"));

        Message delivered = queue.dequeue();
        System.out.println("Delivered: " + delivered);

        System.out.println("Queue size: " + queue.size());

    }
}
