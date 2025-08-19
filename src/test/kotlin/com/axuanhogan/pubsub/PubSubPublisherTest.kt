package com.axuanhogan.pubsub

import com.axuanhogan.pubsub.config.PubSubConfig
import com.axuanhogan.pubsub.exception.PubSubException
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.api.core.ApiFuture
import com.google.cloud.pubsub.v1.Publisher
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.TopicName
import io.mockk.*
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import java.util.concurrent.TimeUnit

class PubSubPublisherTest {

    private lateinit var pubSubPublisher: PubSubPublisher
    private lateinit var mockPubSubConfig: PubSubConfig
    private lateinit var mockObjectMapper: ObjectMapper
    private lateinit var mockPublisher: Publisher

    data class TestMessage(val content: String)

    @BeforeEach
    fun setup() {
        mockPubSubConfig = mockk<PubSubConfig>()
        mockObjectMapper = mockk<ObjectMapper>()
        mockPublisher = mockk<Publisher>()

        every { mockPubSubConfig.projectId } returns "test-project"
        every { mockPubSubConfig.enableOrdering } returns false

        pubSubPublisher = PubSubPublisher()
        pubSubPublisher.pubSubConfig = mockPubSubConfig
        pubSubPublisher.objectMapper = mockObjectMapper

        // Mock Publisher.newBuilder static method
        mockkStatic(Publisher::class)
        val mockBuilder = mockk<Publisher.Builder>()
        every { Publisher.newBuilder(any<TopicName>()) } returns mockBuilder
        every { mockBuilder.setEnableMessageOrdering(any()) } returns mockBuilder
        every { mockBuilder.build() } returns mockPublisher
    }

    @Test
    fun `publishAsync should publish message successfully`() = runBlocking {
        // Given
        val message = TestMessage("test content")
        val topicName = "test-topic"
        val messageId = "msg-123"
        val messageJson = """{"content":"test content"}"""

        every { mockObjectMapper.writeValueAsString(message) } returns messageJson

        val mockFuture = mockk<ApiFuture<String>>()
        every { mockPublisher.publish(any()) } returns mockFuture
        every { mockFuture.get() } returns messageId

        // When
        val result = pubSubPublisher.publishAsync(topicName, message)

        // Then
        assertEquals(messageId, result)
        verify(exactly = 1) { mockObjectMapper.writeValueAsString(message) }
        verify(exactly = 1) { mockPublisher.publish(any()) }
    }

    @Test
    fun `publishAsync should publish message with attributes`() = runBlocking {
        // Given
        val message = TestMessage("test content")
        val topicName = "test-topic"
        val messageId = "msg-123"
        val messageJson = """{"content":"test content"}"""
        val attributes = mapOf("key1" to "value1", "key2" to "value2")

        every { mockObjectMapper.writeValueAsString(message) } returns messageJson

        val mockFuture = mockk<ApiFuture<String>>()
        every { mockPublisher.publish(any()) } returns mockFuture
        every { mockFuture.get() } returns messageId

        // When
        val result = pubSubPublisher.publishAsync(topicName, message, attributes)

        // Then
        assertEquals(messageId, result)

        // Verify the PubsubMessage contains the expected attributes
        val messageCapture = slot<PubsubMessage>()
        verify { mockPublisher.publish(capture(messageCapture)) }

        val capturedMessage = messageCapture.captured
        assertTrue(capturedMessage.attributesMap.containsKey("key1"))
        assertTrue(capturedMessage.attributesMap.containsKey("key2"))
        assertTrue(capturedMessage.attributesMap.containsKey("messageJob"))
        assertTrue(capturedMessage.attributesMap.containsKey("timestamp"))
        assertEquals("value1", capturedMessage.attributesMap["key1"])
        assertEquals("value2", capturedMessage.attributesMap["key2"])
    }

    @Test
    fun `publishAsync should publish message with ordering key when enabled`() = runBlocking {
        // Given
        val message = TestMessage("test content")
        val topicName = "test-topic"
        val messageId = "msg-123"
        val messageJson = """{"content":"test content"}"""
        val orderingKey = "order-123"

        every { mockPubSubConfig.enableOrdering } returns true
        every { mockObjectMapper.writeValueAsString(message) } returns messageJson

        val mockFuture = mockk<ApiFuture<String>>()
        every { mockPublisher.publish(any()) } returns mockFuture
        every { mockFuture.get() } returns messageId

        // When
        val result = pubSubPublisher.publishAsync(topicName, message, emptyMap(), orderingKey)

        // Then
        assertEquals(messageId, result)

        val messageCapture = slot<PubsubMessage>()
        verify { mockPublisher.publish(capture(messageCapture)) }

        val capturedMessage = messageCapture.captured
        assertEquals(orderingKey, capturedMessage.orderingKey)
    }

    @Test
    fun `publishAsync should not set ordering key when disabled`() = runBlocking {
        // Given
        val message = TestMessage("test content")
        val topicName = "test-topic"
        val messageId = "msg-123"
        val messageJson = """{"content":"test content"}"""
        val orderingKey = "order-123"

        every { mockPubSubConfig.enableOrdering } returns false
        every { mockObjectMapper.writeValueAsString(message) } returns messageJson

        val mockFuture = mockk<ApiFuture<String>>()
        every { mockPublisher.publish(any()) } returns mockFuture
        every { mockFuture.get() } returns messageId

        // When
        val result = pubSubPublisher.publishAsync(topicName, message, emptyMap(), orderingKey)

        // Then
        assertEquals(messageId, result)

        val messageCapture = slot<PubsubMessage>()
        verify { mockPublisher.publish(capture(messageCapture)) }

        val capturedMessage = messageCapture.captured
        assertEquals("", capturedMessage.orderingKey) // Should be empty when ordering is disabled
    }

    @Test
    fun `publishAsync should throw PubSubException when publisher fails`(): Unit = runBlocking {
        // Given
        val message = TestMessage("test content")
        val topicName = "test-topic"
        val messageJson = """{"content":"test content"}"""

        every { mockObjectMapper.writeValueAsString(message) } returns messageJson

        val mockFuture = mockk<ApiFuture<String>>()
        every { mockPublisher.publish(any()) } returns mockFuture
        every { mockFuture.get() } throws RuntimeException("Publisher error")

        // When & Then
        assertThrows<PubSubException> {
            runBlocking { pubSubPublisher.publishAsync(topicName, message) }
        }
    }

    @Test
    fun `publishAsync should throw PubSubException when JSON serialization fails`(): Unit = runBlocking {
        // Given
        val message = TestMessage("test content")
        val topicName = "test-topic"

        every { mockObjectMapper.writeValueAsString(message) } throws RuntimeException("JSON error")

        // When & Then
        assertThrows<RuntimeException> {
            runBlocking { pubSubPublisher.publishAsync(topicName, message) }
        }
    }

    @Test
    fun `publishFireAndForget should publish message successfully`() {
        // Given
        val message = TestMessage("test content")
        val topicName = "test-topic"
        val messageJson = """{"content":"test content"}"""

        every { mockObjectMapper.writeValueAsString(message) } returns messageJson

        val mockFuture = mockk<ApiFuture<String>>()
        every { mockPublisher.publish(any()) } returns mockFuture
        every { mockFuture.get() } returns "msg-123"
        every { mockFuture.addListener(any(), any()) } just Runs

        // When
        pubSubPublisher.publishFireAndForget(topicName, message)

        // Then
        // Need to wait a bit for the CompletableFuture to execute
        Thread.sleep(100)

        verify(exactly = 1) { mockObjectMapper.writeValueAsString(message) }
        verify(exactly = 1) { mockPublisher.publish(any()) }
    }

    @Test
    fun `publishFireAndForget should handle exceptions gracefully`() {
        // Given
        val message = TestMessage("test content")
        val topicName = "test-topic"

        every { mockObjectMapper.writeValueAsString(message) } throws RuntimeException("JSON error")

        // When - Should not throw exception
        pubSubPublisher.publishFireAndForget(topicName, message)

        // Then - Should complete without throwing
        Thread.sleep(100)
        verify(exactly = 1) { mockObjectMapper.writeValueAsString(message) }
    }

    @Test
    fun `getOrCreatePublisher should reuse existing publisher`() = runBlocking {
        // Given
        val message1 = TestMessage("test1")
        val message2 = TestMessage("test2")
        val topicName = "test-topic"
        val messageJson1 = """{"content":"test1"}"""
        val messageJson2 = """{"content":"test2"}"""

        every { mockObjectMapper.writeValueAsString(message1) } returns messageJson1
        every { mockObjectMapper.writeValueAsString(message2) } returns messageJson2

        val mockFuture = mockk<ApiFuture<String>>()
        every { mockPublisher.publish(any()) } returns mockFuture
        every { mockFuture.get() } returns "msg-123"

        // When
        pubSubPublisher.publishAsync(topicName, message1)
        pubSubPublisher.publishAsync(topicName, message2)

        // Then - Publisher.newBuilder should be called only once for the same topic
        verify(exactly = 1) { Publisher.newBuilder(TopicName.of("test-project", topicName)) }
        verify(exactly = 2) { mockPublisher.publish(any()) }
    }

    @Test
    fun `shutdown should shutdown all publishers`() {
        // Given - Setup some publishers first
        val message = TestMessage("test")
        val messageJson = """{"content":"test"}"""

        every { mockObjectMapper.writeValueAsString(message) } returns messageJson
        every { mockPublisher.shutdown() } just Runs
        every { mockPublisher.awaitTermination(30, TimeUnit.SECONDS) } returns true

        val mockFuture = mockk<ApiFuture<String>>()
        every { mockPublisher.publish(any()) } returns mockFuture
        every { mockFuture.get() } returns "msg-123"

        // Create some publishers
        runBlocking {
            pubSubPublisher.publishAsync("topic1", message)
            pubSubPublisher.publishAsync("topic2", message)
        }

        // When
        pubSubPublisher.shutdown()

        // Then
        verify(exactly = 2) { mockPublisher.shutdown() }
        verify(exactly = 2) { mockPublisher.awaitTermination(30, TimeUnit.SECONDS) }
    }

    @Test
    fun `shutdown should handle publisher termination timeout`() {
        // Given - Setup a publisher first
        val message = TestMessage("test")
        val messageJson = """{"content":"test"}"""

        every { mockObjectMapper.writeValueAsString(message) } returns messageJson
        every { mockPublisher.shutdown() } just Runs
        every { mockPublisher.awaitTermination(30, TimeUnit.SECONDS) } returns false // Timeout

        val mockFuture = mockk<ApiFuture<String>>()
        every { mockPublisher.publish(any()) } returns mockFuture
        every { mockFuture.get() } returns "msg-123"

        // Create a publisher
        runBlocking {
            pubSubPublisher.publishAsync("topic1", message)
        }

        // When
        pubSubPublisher.shutdown()

        // Then
        verify(exactly = 1) { mockPublisher.shutdown() }
        verify(exactly = 1) { mockPublisher.awaitTermination(30, TimeUnit.SECONDS) }
    }

    @Test
    fun `shutdown should handle exceptions during shutdown`() {
        // Given - Setup a publisher first
        val message = TestMessage("test")
        val messageJson = """{"content":"test"}"""

        every { mockObjectMapper.writeValueAsString(message) } returns messageJson
        every { mockPublisher.shutdown() } throws RuntimeException("Shutdown error")

        val mockFuture = mockk<ApiFuture<String>>()
        every { mockPublisher.publish(any()) } returns mockFuture
        every { mockFuture.get() } returns "msg-123"

        // Create a publisher
        runBlocking {
            pubSubPublisher.publishAsync("topic1", message)
        }

        // When - Should not throw exception
        pubSubPublisher.shutdown()

        // Then
        verify(exactly = 1) { mockPublisher.shutdown() }
    }
}
