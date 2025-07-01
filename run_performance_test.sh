#!/bin/bash

echo "Message Queue Performance Test"
echo "=============================="
echo

# Clean and compile the project
echo "Compiling project..."
mvn clean compile test-compile -q

if [ $? -ne 0 ]; then
    echo "Error: Compilation failed"
    exit 1
fi

echo "Compilation successful!"
echo

# Run the performance test
echo "Starting performance test..."
echo

# Run the test
mvn exec:java -Dexec.mainClass="com.ofek.queue.PerformanceTest" -Dexec.classpathScope="test" -q

if [ $? -eq 0 ]; then
    echo
    echo "Performance test completed successfully!"
    echo
else
    echo "Error: Performance test failed"
    exit 1
fi
