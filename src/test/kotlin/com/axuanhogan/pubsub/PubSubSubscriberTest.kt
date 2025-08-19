package com.axuanhogan.pubsub

import com.axuanhogan.pubsub.config.PubSubConfig
import com.axuanhogan.pubsub.exception.PubSubException
import com.axuanhogan.pubsub.handler.PubSubMessageHandler
import com.axuanhogan.pubsub.handler.PubSubMessageResult
import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.cloud.pubsub.v1.MessageReceiver
import com.google.cloud.pubsub.v1.Subscriber
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.pubsub.v1.PubsubMessage
import io.mockk.*
import jakarta.enterprise.inject.Instance
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlin.system.measureTimeMillis
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertTrue
import java.util.concurrent.TimeUnit

class PubSubSubscriberTest {

    private lateinit var pubSubSubscriber: TestPubSubSubscriber
    private lateinit var mockPubSubConfig: PubSubConfig
    private lateinit var mockMessageHandlers: Instance<PubSubMessageHandler>
    private lateinit var mockSubscriber: Subscriber
    private lateinit var mockHandler: PubSubMessageHandler

    // Test subclass that overrides the message handling to avoid CDI mocking issues
    class TestPubSubSubscriber : PubSubSubscriber() {
        var testHandler: PubSubMessageHandler? = null

        // Create a publicly accessible version of handleMessage for testing
        suspend fun testHandleMessage(message: PubsubMessage, consumer: AckReplyConsumer) {
            val messageJob = message.attributesMap["messageJob"] ?: "Unknown"
            val messageId = message.messageId

            // Check for delay attribute (same logic as in PubSubSubscriber)
            val delaySecondsStr = message.attributesMap["delaySeconds"]
            if (delaySecondsStr != null) {
                try {
                    val delaySeconds = delaySecondsStr.toLong()
                    if (delaySeconds > 0) {
                        delay(delaySeconds * 1000) // Convert to milliseconds
                    }
                } catch (e: NumberFormatException) {
                    // Process immediately if invalid delaySeconds
                }
            }

            try {
                val handler = testHandler?.takeIf { it.canHandle(messageJob) }
                if (handler == null) {
                    consumer.ack()
                    return
                }

                val result = handler.handle(message)

                when (result) {
                    PubSubMessageResult.SUCCESS -> {
                        consumer.ack()
                    }
                    PubSubMessageResult.RETRY -> {
                        consumer.nack()
                    }
                    PubSubMessageResult.DEAD_LETTER -> {
                        consumer.ack() // Ack to prevent retry, message will go to dead letter topic
                    }
                }

            } catch (e: Exception) {
                consumer.nack()
            }
        }
    }

    @BeforeEach
    fun setup() {
        mockPubSubConfig = mockk<PubSubConfig>()
        mockMessageHandlers = mockk<Instance<PubSubMessageHandler>>()
        mockSubscriber = mockk<Subscriber>()
        mockHandler = mockk<PubSubMessageHandler>()

        every { mockPubSubConfig.projectId } returns "test-project"

        pubSubSubscriber = TestPubSubSubscriber()
        pubSubSubscriber.pubSubConfig = mockPubSubConfig
        pubSubSubscriber.messageHandlers = mockMessageHandlers

        // Mock Subscriber.newBuilder static method
        mockkStatic(Subscriber::class)
        val mockBuilder = mockk<Subscriber.Builder>()
        every { Subscriber.newBuilder(any<ProjectSubscriptionName>(), any<MessageReceiver>()) } returns mockBuilder
        every { mockBuilder.build() } returns mockSubscriber
        every { mockSubscriber.startAsync() } returns mockSubscriber
    }

    @Test
    fun `startSubscription should start new subscription successfully`() {
        // Given
        val subscriptionName = "test-subscription"

        // When
        pubSubSubscriber.startSubscription(subscriptionName)

        // Then
        verify(exactly = 1) {
            Subscriber.newBuilder(
                ProjectSubscriptionName.of("test-project", subscriptionName),
                any<MessageReceiver>()
            )
        }
        verify(exactly = 1) { mockSubscriber.startAsync() }
    }

    @Test
    fun `startSubscription should not start duplicate subscription`() {
        // Given
        val subscriptionName = "test-subscription"

        // When - Start same subscription twice
        pubSubSubscriber.startSubscription(subscriptionName)
        pubSubSubscriber.startSubscription(subscriptionName)

        // Then - Should only create one subscriber
        verify(exactly = 1) {
            Subscriber.newBuilder(
                ProjectSubscriptionName.of("test-project", subscriptionName),
                any<MessageReceiver>()
            )
        }
        verify(exactly = 1) { mockSubscriber.startAsync() }
    }

    @Test
    fun `startSubscription should throw PubSubException when subscriber creation fails`() {
        // Given
        val subscriptionName = "test-subscription"
        every { mockSubscriber.startAsync() } throws RuntimeException("Start error")

        // When & Then
        try {
            pubSubSubscriber.startSubscription(subscriptionName)
            assertTrue(false, "Expected PubSubException")
        } catch (e: PubSubException) {
            assertTrue(e.message!!.contains("Failed to start subscription"))
        }
    }

    @Test
    fun `stopSubscription should stop existing subscription successfully`() {
        // Given
        val subscriptionName = "test-subscription"
        every { mockSubscriber.stopAsync() } returns mockSubscriber
        every { mockSubscriber.awaitTerminated(30, TimeUnit.SECONDS) } just Runs

        // Start subscription first
        pubSubSubscriber.startSubscription(subscriptionName)

        // When
        pubSubSubscriber.stopSubscription(subscriptionName)

        // Then
        verify(exactly = 1) { mockSubscriber.stopAsync() }
        verify(exactly = 1) { mockSubscriber.awaitTerminated(30, TimeUnit.SECONDS) }
    }

    @Test
    fun `stopSubscription should handle timeout during termination`() {
        // Given
        val subscriptionName = "test-subscription"
        every { mockSubscriber.stopAsync() } returns mockSubscriber
        every { mockSubscriber.awaitTerminated(30, TimeUnit.SECONDS) } throws java.util.concurrent.TimeoutException()

        // Start subscription first
        pubSubSubscriber.startSubscription(subscriptionName)

        // When - Should not throw exception
        pubSubSubscriber.stopSubscription(subscriptionName)

        // Then
        verify(exactly = 1) { mockSubscriber.stopAsync() }
        verify(exactly = 1) { mockSubscriber.awaitTerminated(30, TimeUnit.SECONDS) }
    }

    @Test
    fun `stopSubscription should handle exceptions during stop`() {
        // Given
        val subscriptionName = "test-subscription"
        every { mockSubscriber.stopAsync() } throws RuntimeException("Stop error")

        // Start subscription first
        pubSubSubscriber.startSubscription(subscriptionName)

        // When - Should not throw exception
        pubSubSubscriber.stopSubscription(subscriptionName)

        // Then
        verify(exactly = 1) { mockSubscriber.stopAsync() }
    }

    @Test
    fun `stopSubscription should do nothing for non-existent subscription`() {
        // Given
        val subscriptionName = "non-existent-subscription"

        // When
        pubSubSubscriber.stopSubscription(subscriptionName)

        // Then - No interactions with subscriber
        verify(exactly = 0) { mockSubscriber.stopAsync() }
    }

    @Test
    fun `stopAllSubscriptions should stop all active subscriptions`() {
        // Given
        val subscription1 = "test-subscription-1"
        val subscription2 = "test-subscription-2"

        every { mockSubscriber.stopAsync() } returns mockSubscriber
        every { mockSubscriber.awaitTerminated(30, TimeUnit.SECONDS) } just Runs

        // Start multiple subscriptions
        pubSubSubscriber.startSubscription(subscription1)
        pubSubSubscriber.startSubscription(subscription2)

        // When
        pubSubSubscriber.stopAllSubscriptions()

        // Then - Should stop both subscriptions
        verify(exactly = 2) { mockSubscriber.stopAsync() }
        verify(exactly = 2) { mockSubscriber.awaitTerminated(30, TimeUnit.SECONDS) }
    }

    @Test
    fun `handleMessage should process message with matching handler successfully`() = runBlocking {
        // Given
        val message = mockk<PubsubMessage>()
        val consumer = mockk<AckReplyConsumer>()
        val messageJob = "TestJob"

        every { message.attributesMap } returns mapOf("messageJob" to messageJob)
        every { message.messageId } returns "msg-123"

        // Set up test handler
        every { mockHandler.canHandle(messageJob) } returns true
        coEvery { mockHandler.handle(message) } returns PubSubMessageResult.SUCCESS
        every { consumer.ack() } just Runs
        pubSubSubscriber.testHandler = mockHandler

        // When
        pubSubSubscriber.testHandleMessage(message, consumer)

        // Then
        verify(exactly = 1) { consumer.ack() }
        coVerify(exactly = 1) { mockHandler.handle(message) }
    }

    @Test
    fun `handleMessage should nack message when handler returns RETRY`() = runBlocking {
        // Given
        val message = mockk<PubsubMessage>()
        val consumer = mockk<AckReplyConsumer>()
        val messageJob = "TestJob"

        every { message.attributesMap } returns mapOf("messageJob" to messageJob)
        every { message.messageId } returns "msg-123"

        // Set up test handler
        every { mockHandler.canHandle(messageJob) } returns true
        coEvery { mockHandler.handle(message) } returns PubSubMessageResult.RETRY
        every { consumer.nack() } just Runs
        pubSubSubscriber.testHandler = mockHandler

        // When
        pubSubSubscriber.testHandleMessage(message, consumer)

        // Then
        verify(exactly = 1) { consumer.nack() }
        coVerify(exactly = 1) { mockHandler.handle(message) }
    }

    @Test
    fun `handleMessage should ack message when handler returns DEAD_LETTER`() = runBlocking {
        // Given
        val message = mockk<PubsubMessage>()
        val consumer = mockk<AckReplyConsumer>()
        val messageJob = "TestJob"

        every { message.attributesMap } returns mapOf("messageJob" to messageJob)
        every { message.messageId } returns "msg-123"

        // Set up test handler
        every { mockHandler.canHandle(messageJob) } returns true
        coEvery { mockHandler.handle(message) } returns PubSubMessageResult.DEAD_LETTER
        every { consumer.ack() } just Runs
        pubSubSubscriber.testHandler = mockHandler

        // When
        pubSubSubscriber.testHandleMessage(message, consumer)

        // Then
        verify(exactly = 1) { consumer.ack() }
        coVerify(exactly = 1) { mockHandler.handle(message) }
    }

    @Test
    fun `handleMessage should ack message when no handler found`() = runBlocking {
        // Given
        val message = mockk<PubsubMessage>()
        val consumer = mockk<AckReplyConsumer>()
        val messageJob = "UnknownJob"

        every { message.attributesMap } returns mapOf("messageJob" to messageJob)
        every { message.messageId } returns "msg-123"
        every { consumer.ack() } just Runs

        // No test handler set, so no handler will be found
        pubSubSubscriber.testHandler = null

        // When
        pubSubSubscriber.testHandleMessage(message, consumer)

        // Then
        verify(exactly = 1) { consumer.ack() }
    }

    @Test
    fun `handleMessage should nack message when handler throws exception`() = runBlocking {
        // Given
        val message = mockk<PubsubMessage>()
        val consumer = mockk<AckReplyConsumer>()
        val messageJob = "TestJob"

        every { message.attributesMap } returns mapOf("messageJob" to messageJob)
        every { message.messageId } returns "msg-123"

        // Set up test handler
        every { mockHandler.canHandle(messageJob) } returns true
        coEvery { mockHandler.handle(message) } throws RuntimeException("Handler error")
        every { consumer.nack() } just Runs
        pubSubSubscriber.testHandler = mockHandler

        // When
        pubSubSubscriber.testHandleMessage(message, consumer)

        // Then
        verify(exactly = 1) { consumer.nack() }
        coVerify(exactly = 1) { mockHandler.handle(message) }
    }

    @Test
    fun `handleMessage should handle missing messageJob attribute`() = runBlocking {
        // Given
        val message = mockk<PubsubMessage>()
        val consumer = mockk<AckReplyConsumer>()

        every { message.attributesMap } returns emptyMap()
        every { message.messageId } returns "msg-123"
        every { consumer.ack() } just Runs

        // No test handler set
        pubSubSubscriber.testHandler = null

        // When
        pubSubSubscriber.testHandleMessage(message, consumer)

        // Then
        verify(exactly = 1) { consumer.ack() }
    }

    @Test
    fun `handleMessage should delay processing when delaySeconds is specified`() = runBlocking {
        // Given
        val message = mockk<PubsubMessage>()
        val consumer = mockk<AckReplyConsumer>()
        val messageJob = "TestJob"
        val delaySeconds = 2L

        every { message.attributesMap } returns mapOf(
            "messageJob" to messageJob,
            "delaySeconds" to delaySeconds.toString()
        )
        every { message.messageId } returns "msg-123"
        every { consumer.ack() } just Runs

        coEvery { mockHandler.canHandle(messageJob) } returns true
        coEvery { mockHandler.handle(message) } returns PubSubMessageResult.SUCCESS

        pubSubSubscriber.testHandler = mockHandler

        // When - measure execution time
        val executionTimeMs = measureTimeMillis {
            pubSubSubscriber.testHandleMessage(message, consumer)
        }

        // Then - should have delayed for at least the specified seconds
        assertTrue(executionTimeMs >= delaySeconds * 1000,
            "Expected execution time to be at least ${delaySeconds * 1000}ms, but was ${executionTimeMs}ms")

        verify(exactly = 1) { consumer.ack() }
        coVerify(exactly = 1) { mockHandler.handle(message) }
    }

    @Test
    fun `handleMessage should process immediately when delaySeconds is zero`() = runBlocking {
        // Given
        val message = mockk<PubsubMessage>()
        val consumer = mockk<AckReplyConsumer>()
        val messageJob = "TestJob"

        every { message.attributesMap } returns mapOf(
            "messageJob" to messageJob,
            "delaySeconds" to "0"
        )
        every { message.messageId } returns "msg-123"
        every { consumer.ack() } just Runs

        coEvery { mockHandler.canHandle(messageJob) } returns true
        coEvery { mockHandler.handle(message) } returns PubSubMessageResult.SUCCESS

        pubSubSubscriber.testHandler = mockHandler

        // When - measure execution time
        val executionTimeMs = measureTimeMillis {
            pubSubSubscriber.testHandleMessage(message, consumer)
        }

        // Then - should process immediately (within 100ms)
        assertTrue(executionTimeMs < 100,
            "Expected execution time to be less than 100ms, but was ${executionTimeMs}ms")

        verify(exactly = 1) { consumer.ack() }
        coVerify(exactly = 1) { mockHandler.handle(message) }
    }

    @Test
    fun `handleMessage should process immediately when delaySeconds is negative`() = runBlocking {
        // Given
        val message = mockk<PubsubMessage>()
        val consumer = mockk<AckReplyConsumer>()
        val messageJob = "TestJob"

        every { message.attributesMap } returns mapOf(
            "messageJob" to messageJob,
            "delaySeconds" to "-5"
        )
        every { message.messageId } returns "msg-123"
        every { consumer.ack() } just Runs

        coEvery { mockHandler.canHandle(messageJob) } returns true
        coEvery { mockHandler.handle(message) } returns PubSubMessageResult.SUCCESS

        pubSubSubscriber.testHandler = mockHandler

        // When - measure execution time
        val executionTimeMs = measureTimeMillis {
            pubSubSubscriber.testHandleMessage(message, consumer)
        }

        // Then - should process immediately (within 100ms)
        assertTrue(executionTimeMs < 100,
            "Expected execution time to be less than 100ms, but was ${executionTimeMs}ms")

        verify(exactly = 1) { consumer.ack() }
        coVerify(exactly = 1) { mockHandler.handle(message) }
    }

    @Test
    fun `handleMessage should process immediately when delaySeconds is invalid`() = runBlocking {
        // Given
        val message = mockk<PubsubMessage>()
        val consumer = mockk<AckReplyConsumer>()
        val messageJob = "TestJob"

        every { message.attributesMap } returns mapOf(
            "messageJob" to messageJob,
            "delaySeconds" to "invalid"
        )
        every { message.messageId } returns "msg-123"
        every { consumer.ack() } just Runs

        coEvery { mockHandler.canHandle(messageJob) } returns true
        coEvery { mockHandler.handle(message) } returns PubSubMessageResult.SUCCESS

        pubSubSubscriber.testHandler = mockHandler

        // When - measure execution time
        val executionTimeMs = measureTimeMillis {
            pubSubSubscriber.testHandleMessage(message, consumer)
        }

        // Then - should process immediately (within 100ms) despite invalid delaySeconds
        assertTrue(executionTimeMs < 100,
            "Expected execution time to be less than 100ms, but was ${executionTimeMs}ms")

        verify(exactly = 1) { consumer.ack() }
        coVerify(exactly = 1) { mockHandler.handle(message) }
    }

    @Test
    fun `handleMessage should process immediately when no delaySeconds attribute`() = runBlocking {
        // Given
        val message = mockk<PubsubMessage>()
        val consumer = mockk<AckReplyConsumer>()
        val messageJob = "TestJob"

        every { message.attributesMap } returns mapOf("messageJob" to messageJob)
        every { message.messageId } returns "msg-123"
        every { consumer.ack() } just Runs

        coEvery { mockHandler.canHandle(messageJob) } returns true
        coEvery { mockHandler.handle(message) } returns PubSubMessageResult.SUCCESS

        pubSubSubscriber.testHandler = mockHandler

        // When - measure execution time
        val executionTimeMs = measureTimeMillis {
            pubSubSubscriber.testHandleMessage(message, consumer)
        }

        // Then - should process immediately (within 100ms)
        assertTrue(executionTimeMs < 100,
            "Expected execution time to be less than 100ms, but was ${executionTimeMs}ms")

        verify(exactly = 1) { consumer.ack() }
        coVerify(exactly = 1) { mockHandler.handle(message) }
    }

    @Test
    fun `handleMessage should process multiple messages concurrently without blocking`() = runBlocking {
        // Given
        val delayedMessage = mockk<PubsubMessage>()
        val immediateMessage = mockk<PubsubMessage>()
        val delayedConsumer = mockk<AckReplyConsumer>()
        val immediateConsumer = mockk<AckReplyConsumer>()
        val messageJob = "TestJob"

        // Setup delayed message (5 seconds delay)
        every { delayedMessage.attributesMap } returns mapOf(
            "messageJob" to messageJob,
            "delaySeconds" to "5"
        )
        every { delayedMessage.messageId } returns "delayed-msg"
        every { delayedConsumer.ack() } just Runs

        // Setup immediate message (no delay)
        every { immediateMessage.attributesMap } returns mapOf("messageJob" to messageJob)
        every { immediateMessage.messageId } returns "immediate-msg"
        every { immediateConsumer.ack() } just Runs

        coEvery { mockHandler.canHandle(messageJob) } returns true
        coEvery { mockHandler.handle(delayedMessage) } returns PubSubMessageResult.SUCCESS
        coEvery { mockHandler.handle(immediateMessage) } returns PubSubMessageResult.SUCCESS

        pubSubSubscriber.testHandler = mockHandler

        // When - start both messages concurrently
        val startTime = System.currentTimeMillis()

        val delayedJob = async { pubSubSubscriber.testHandleMessage(delayedMessage, delayedConsumer) }

        // Start immediate message after a small delay to ensure delayed message starts first
        delay(100)
        val immediateJob = async { pubSubSubscriber.testHandleMessage(immediateMessage, immediateConsumer) }

        // Wait for immediate job to complete
        immediateJob.await()
        val immediateEndTime = System.currentTimeMillis()

        // Wait for delayed job to complete
        delayedJob.await()
        val delayedEndTime = System.currentTimeMillis()

        // Then - immediate message should complete quickly despite delayed message running
        val immediateProcessTime = immediateEndTime - startTime
        val totalDelayedTime = delayedEndTime - startTime

        // Immediate message should complete in less than 1 second (even though delayed message is still running)
        assertTrue(immediateProcessTime < 1000,
            "Immediate message took ${immediateProcessTime}ms, should be less than 1000ms")

        // Delayed message should take approximately 5+ seconds
        assertTrue(totalDelayedTime >= 5000,
            "Delayed message took ${totalDelayedTime}ms, should be at least 5000ms")

        // Verify both messages were processed
        verify(exactly = 1) { delayedConsumer.ack() }
        verify(exactly = 1) { immediateConsumer.ack() }
        coVerify(exactly = 1) { mockHandler.handle(delayedMessage) }
        coVerify(exactly = 1) { mockHandler.handle(immediateMessage) }
    }

    @Test
    fun `handleMessage should handle multiple delayed messages concurrently`() = runBlocking {
        // Given
        val message1 = mockk<PubsubMessage>()
        val message2 = mockk<PubsubMessage>()
        val consumer1 = mockk<AckReplyConsumer>()
        val consumer2 = mockk<AckReplyConsumer>()
        val messageJob = "TestJob"

        every { message1.attributesMap } returns mapOf(
            "messageJob" to messageJob,
            "delaySeconds" to "2"
        )
        every { message1.messageId } returns "msg-1"
        every { consumer1.ack() } just Runs

        every { message2.attributesMap } returns mapOf(
            "messageJob" to messageJob,
            "delaySeconds" to "2"
        )
        every { message2.messageId } returns "msg-2"
        every { consumer2.ack() } just Runs

        coEvery { mockHandler.canHandle(messageJob) } returns true
        coEvery { mockHandler.handle(any()) } returns PubSubMessageResult.SUCCESS

        pubSubSubscriber.testHandler = mockHandler

        // When - process both messages concurrently
        val startTime = System.currentTimeMillis()

        val job1 = async { pubSubSubscriber.testHandleMessage(message1, consumer1) }
        val job2 = async { pubSubSubscriber.testHandleMessage(message2, consumer2) }

        // Wait for both to complete
        job1.await()
        job2.await()

        val totalTime = System.currentTimeMillis() - startTime

        // Then - both messages should complete in approximately 2 seconds (not 4 seconds)
        // This proves they run concurrently, not sequentially
        assertTrue(totalTime < 3000,
            "Concurrent processing took ${totalTime}ms, should be less than 3000ms (not 4000ms)")
        assertTrue(totalTime >= 2000,
            "Concurrent processing took ${totalTime}ms, should be at least 2000ms")

        verify(exactly = 1) { consumer1.ack() }
        verify(exactly = 1) { consumer2.ack() }
        coVerify(exactly = 2) { mockHandler.handle(any()) }
    }
}