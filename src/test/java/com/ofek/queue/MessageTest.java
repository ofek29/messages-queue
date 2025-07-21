package com.ofek.queue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;
import java.nio.charset.StandardCharsets;

@DisplayName("Message Tests")
public class MessageTest {

    @Test
    @DisplayName("Should handle empty payload")
    void testEmptyPayload() {
        Message message = new Message("");

        assertEquals("", message.getPayloadAsString());
        assertArrayEquals("".getBytes(StandardCharsets.UTF_8), message.getPayload());
    }

    @Test
    @DisplayName("Should handle very long payload")
    void testLongPayload() {
        StringBuilder longPayload = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            longPayload.append("This is a very long payload. ");
        }

        Message message = new Message(longPayload.toString());

        assertEquals(longPayload.toString(), message.getPayloadAsString());
        assertArrayEquals(longPayload.toString().getBytes(StandardCharsets.UTF_8), message.getPayload());
    }

    @Test
    @DisplayName("Should have proper toString representation")
    void testToString() {
        Message message = new Message("test payload");
        String toString = message.toString();

        assertTrue(toString.contains("test payload"));
    }

    @Test
    @DisplayName("Should handle special characters in payload")
    void testSpecialCharacters() {
        String specialPayload = "Special chars: !@#$%^&*()_+[]{}|;:,.<>?`~";
        Message message = new Message(specialPayload);

        assertEquals(specialPayload, message.getPayloadAsString());
        assertArrayEquals(specialPayload.getBytes(StandardCharsets.UTF_8), message.getPayload());
    }

    @Test
    @DisplayName("Should handle newlines and tabs in payload")
    void testNewlinesAndTabs() {
        String payload = "Line 1\nLine 2\tTabbed content\r\nWindows line ending";
        Message message = new Message(payload);

        assertEquals(payload, message.getPayloadAsString());
        assertArrayEquals(payload.getBytes(StandardCharsets.UTF_8), message.getPayload());
    }

    @Test
    @DisplayName("Should handle byte array constructor")
    void testByteArrayConstructor() {
        byte[] originalBytes = "Hello World".getBytes(StandardCharsets.UTF_8);
        Message message = new Message(originalBytes);

        assertArrayEquals(originalBytes, message.getPayload());
        assertEquals("Hello World", message.getPayloadAsString());
    }

    @Test
    @DisplayName("Should handle empty byte array")
    void testEmptyByteArray() {
        byte[] emptyBytes = new byte[0];
        Message message = new Message(emptyBytes);

        assertArrayEquals(emptyBytes, message.getPayload());
        assertEquals("", message.getPayloadAsString());
    }

    @Test
    @DisplayName("Should handle large byte array")
    void testLargeByteArray() {
        byte[] largeBytes = new byte[50000];
        for (int i = 0; i < largeBytes.length; i++) {
            largeBytes[i] = (byte) (i % 256);
        }

        Message message = new Message(largeBytes);

        assertArrayEquals(largeBytes, message.getPayload());
        assertEquals(50000, message.getPayload().length);
    }

    @Test
    @DisplayName("Should handle UTF-8 encoded bytes correctly")
    void testUtf8EncodedBytes() {
        String unicodeText = "Hello 世界 🌍 مرحبا";
        byte[] utf8Bytes = unicodeText.getBytes(StandardCharsets.UTF_8);

        Message message = new Message(utf8Bytes);

        assertArrayEquals(utf8Bytes, message.getPayload());
        assertEquals(unicodeText, message.getPayloadAsString());
    }

    @Test
    @DisplayName("Should handle control characters in byte array")
    void testControlCharactersInByteArray() {
        byte[] controlChars = {
                0, // NULL
                9, // TAB
                10, // LF (newline)
                13, // CR (carriage return)
                27, // ESC
                127 // DEL
        };

        Message message = new Message(controlChars);

        assertArrayEquals(controlChars, message.getPayload());
        // Should not crash when converting to string
        String result = message.getPayloadAsString();
        assertNotNull(result);
    }

    @Test
    @DisplayName("Should preserve message immutability")
    void testMessageImmutability() {
        byte[] originalBytes = "test".getBytes(StandardCharsets.UTF_8);
        Message message = new Message(originalBytes);

        // Modify the original array
        originalBytes[0] = (byte) 'X';

        // Message should not be affected
        assertNotEquals(originalBytes, message.getPayload());
    }

    @Test
    @DisplayName("Should handle byte array vs string constructor equivalence")
    void testByteArrayStringEquivalence() {
        String testString = "Test message with special chars: àáâãäå";

        Message messageFromString = new Message(testString);
        Message messageFromBytes = new Message(testString.getBytes(StandardCharsets.UTF_8));

        assertArrayEquals(messageFromString.getPayload(), messageFromBytes.getPayload());
        assertEquals(messageFromString.getPayloadAsString(), messageFromBytes.getPayloadAsString());
    }
}
