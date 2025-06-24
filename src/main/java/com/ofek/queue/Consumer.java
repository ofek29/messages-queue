package com.ofek.queue;

public class Consumer {
    private final MessageQueue messageQueue;

    public Consumer(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    public Message poll() {
        return messageQueue.dequeue();
    }

}
