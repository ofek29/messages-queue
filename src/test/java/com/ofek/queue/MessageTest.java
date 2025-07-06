package com.ofek.queue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Message Tests")
public class MessageTest {

    @Test
    @DisplayName("Should create message with auto-generated ID")
    void testMessageCreationWithAutoId() {
        String payload = "Test payload";
        Message message = new Message(payload);

        assertNotNull(message.getId());
        assertFalse(message.getId().isEmpty());
        assertEquals(payload, message.getPayload());
    }

    @Test
    @DisplayName("Should create message with specified ID")
    void testMessageCreationWithSpecificId() {
        String id = "custom-id-123";
        String payload = "Test payload";
        Message message = new Message(id, payload);

        assertEquals(id, message.getId());
        assertEquals(payload, message.getPayload());
    }

    @Test
    @DisplayName("Should generate unique IDs for different messages")
    void testUniqueIdGeneration() {
        Message message1 = new Message("Same payload");
        Message message2 = new Message("Same payload");
        Message message3 = new Message("Same payload");

        assertNotEquals(message1.getId(), message2.getId());
        assertNotEquals(message2.getId(), message3.getId());
        assertNotEquals(message1.getId(), message3.getId());
    }

    @Test
    @DisplayName("Should handle empty payload")
    void testEmptyPayload() {
        Message message = new Message("");

        assertNotNull(message.getId());
        assertEquals("", message.getPayload());
    }

    @Test
    @DisplayName("Should handle very long payload")
    void testLongPayload() {
        StringBuilder longPayload = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            longPayload.append("This is a very long payload. ");
        }

        Message message = new Message(longPayload.toString());

        assertNotNull(message.getId());
        assertEquals(longPayload.toString(), message.getPayload());
    }

    @Test
    @DisplayName("Should have proper toString representation")
    void testToString() {
        Message message = new Message("test-id", "test payload");
        String toString = message.toString();

        assertTrue(toString.contains("test-id"));
        assertTrue(toString.contains("test payload"));
        assertTrue(toString.contains("Message{"));
    }

    @Test
    @DisplayName("Should handle special characters in payload")
    void testSpecialCharacters() {
        String specialPayload = "Special chars: !@#$%^&*()_+[]{}|;:,.<>?`~";
        Message message = new Message(specialPayload);

        assertEquals(specialPayload, message.getPayload());
    }

    @Test
    @DisplayName("Should handle newlines and tabs in payload")
    void testNewlinesAndTabs() {
        String payload = "Line 1\nLine 2\tTabbed content\r\nWindows line ending";
        Message message = new Message(payload);

        assertEquals(payload, message.getPayload());
    }

    @Test
    @DisplayName("Should handle pipe character in payload (potential file format conflict)")
    void testPipeCharacterInPayload() {
        String payload = "This|has|pipe|characters";
        Message message = new Message(payload);

        assertEquals(payload, message.getPayload());
    }
}
