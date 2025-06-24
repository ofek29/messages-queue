# messages-queue

A simple Java-based message queue implementation with file storage.

## Features

- **Thread-safe message queue** using `BlockingQueue`
- **Unique message IDs** - automatically generated UUIDs for each message
- **File-based storage** - messages survive application restarts
- **Producer/Consumer pattern** - separate classes for producing and consuming messages

## Classes

### [`Message`](src/main/java/com/ofek/queue/Message.java)

Represents a message with:

- Unique ID (UUID)
- String payload
- Two constructors:
  - `Message(String payload)` - creates new message with generated ID
  - `Message(String id, String payload)` - loads existing message with preserved ID

### [`MessageQueue`](src/main/java/com/ofek/queue/MessageQueue.java)

The main messages queue implementation with:

- `enqueue(Message message)` - adds message to queue and saves to file
- `dequeue()` - removes and returns message from queue
- `size()` - returns current queue size
- Automatic file loading on startup
- Saves messages to a specified file

### [`Producer`](src/main/java/com/ofek/queue/Producer.java)

Handles message production with:

- `produce(String payload)` - creates and enqueues a new message
- Wraps the message creation and enqueuing process

### [`Consumer`](src/main/java/com/ofek/queue/Consumer.java)

Handles message consumption with:

- `poll()` - retrieves and returns the next message from the queue
- Wraps the message dequeuing process

### [`App`](src/main/java/com/ofek/queue/App.java)

Demo application showing producer/consumer usage pattern

## Usage

```java
// Create a message queue with file storage
MessageQueue queue = new MessageQueue("messages.log");

// Create producer and consumer
Producer producer = new Producer(queue);
Consumer consumer = new Consumer(queue);

// Produce messages
producer.produce("Hello, World!");
producer.produce("Second message");

// Consume messages
Message delivered = consumer.poll();
System.out.println("Delivered: " + delivered);

// Check queue size
int size = queue.size();
```

## Building and Running

This is a Maven project. To build and run:

```bash
# Compile
mvn compile

# Run the demo
mvn exec:java -Dexec.mainClass="com.ofek.queue.App"

# Run tests
mvn test
```

## File Format

Messages are stored in pipe-delimited format:

```
UUID|payload
another-uuid|another message
```

## Requirements

- Java 21
- Maven 3.x
