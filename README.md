# messages-queue

A simple Java-based message queue implementation with file storage.

## Features

- **Thread-safe message queue** using `BlockingQueue` with read-write locks
- **Unique message IDs** - automatically generated UUIDs for each message
- **Async batch persistence** - high-performance file storage with configurable batch sizes
- **File-based storage** - messages survive application restarts with automatic loading
- **Producer/Consumer pattern** - separate classes for producing and consuming messages
- **Configurable settings** - customizable batch size and logging options

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

- `enqueue(Message message)` - adds message to queue with async persistence
- `dequeue()` - removes and returns message from queue
- `size()` - returns current queue size
- `flush()` - forces all pending messages to be written to disk
- `shutdown()` - gracefully shuts down the queue, ensuring all messages are persisted
- **Async batch persistence** - messages are written to file in configurable batches for optimal performance
- **Configurable settings** - batch size and console logging can be customized
- Automatic file loading on startup
- Thread-safe operations with read-write locks

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

### Basic Usage

```java
// Create a message queue with default settings (batch size: 100, no console logging)
MessageQueue queue = new MessageQueue("messages.log");

// Or create with custom configuration
MessageQueue queue = new MessageQueue("messages.log", 50, true); // batch size: 50, console logging enabled

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

// Gracefully shutdown (automatically flushes pending messages)
queue.shutdown();
```

### Configuration Options

The MessageQueue constructor accepts the following parameters:

- **filename** (String): The file path where messages will be persisted
- **batchSize** (int, optional): Number of messages to batch before writing to file (default: 100)
- **enableConsoleLogging** (boolean, optional): Whether to log enqueue operations to console (default: false)

### Async Batch Persistence

The MessageQueue implements an efficient async batch persistence mechanism:

- **Non-blocking enqueue**: Messages are added to the queue immediately without waiting for disk I/O
- **Background persistence**: A dedicated thread handles writing messages to file
- **Batch optimization**: Messages are written in configurable batches to reduce file I/O overhead
- **Automatic flushing**: The system ensures data integrity by flushing batches to disk
- **Graceful shutdown**: Call `shutdown()` to ensure all pending messages are persisted (includes automatic flush)

This design provides high throughput for message enqueuing while maintaining data durability.

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

## Performance Testing

This project includes comprehensive performance testing tools to measure queue performance under various conditions.

### Quick Performance Test

Run the automated performance test script:

```bash
# Make script executable (first time only)
chmod +x run_performance_test.sh

# Run performance tests
./run_performance_test.sh
```

This will:

- Test with 1K, 10K, 100K, and 1M messages
- Run both single-threaded and multi-threaded tests
- Generate a detailed performance report (`performance_report.log`)

### Manual Performance Tests

#### Basic Performance Test

```bash
# Compile test classes
mvn test-compile

# Run basic performance test
mvn exec:java -Dexec.mainClass="com.ofek.queue.PerformanceTest" -Dexec.classpathScope="test"
```

### Performance Test Outputs

The performance tests generate multiple output files:

**`performance_report.log`** - Human-readable performance report with:

- System information (Java version, OS, memory)
- Test results for different message counts
- Single-threaded and multi-threaded performance metrics
- Memory usage information

### Sample Performance Results

Typical results on a modern system:

```
Messages: 100,000 | Enqueue: 2,450.32 ms | Dequeue: 1,234.56 ms | Total: 3,684.88 ms | Throughput: 27,140.23 msg/sec
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
