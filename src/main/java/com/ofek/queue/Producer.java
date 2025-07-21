package com.ofek.queue;

public class Producer {
    private final MessageQueue messageQueue;

    public Producer(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    public void produce(String payload) {
        Message message = new Message(payload);
        messageQueue.enqueue(message);
    }

    public void produce(byte[] payload) {
        Message message = new Message(payload);
        messageQueue.enqueue(message);
    }

}
