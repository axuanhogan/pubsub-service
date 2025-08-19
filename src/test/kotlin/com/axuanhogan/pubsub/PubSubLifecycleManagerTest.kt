package com.axuanhogan.pubsub

import io.quarkus.runtime.ShutdownEvent
import io.quarkus.runtime.StartupEvent
import io.mockk.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import com.axuanhogan.pubsub.exception.PubSubException

class PubSubLifecycleManagerTest {

    private lateinit var pubSubLifecycleManager: PubSubLifecycleManager
    private lateinit var mockPubSubSubscriber: PubSubSubscriber
    private lateinit var mockPubSubPublisher: PubSubPublisher
    private lateinit var mockStartupEvent: StartupEvent
    private lateinit var mockShutdownEvent: ShutdownEvent

    @BeforeEach
    fun setup() {
        mockPubSubSubscriber = mockk<PubSubSubscriber>()
        mockPubSubPublisher = mockk<PubSubPublisher>()
        mockStartupEvent = mockk<StartupEvent>()
        mockShutdownEvent = mockk<ShutdownEvent>()

        pubSubLifecycleManager = PubSubLifecycleManager()
        pubSubLifecycleManager.pubSubSubscriber = mockPubSubSubscriber
        pubSubLifecycleManager.pubSubPublisher = mockPubSubPublisher
    }

    @Test
    fun `onStart should start subscriptions successfully`() {
        // Given
        every { mockPubSubSubscriber.startSubscription(any()) } just Runs

        // When
        pubSubLifecycleManager.onStart(mockStartupEvent)

        // Then
        verify(exactly = 1) {
            mockPubSubSubscriber.startSubscription("application.test.topic-hello.pubsub")
        }
    }

    @Test
    fun `onStart should throw exception when subscription fails`() {
        // Given
        val expectedException = PubSubException("Failed to start subscription")
        every { mockPubSubSubscriber.startSubscription(any()) } throws expectedException

        // When & Then
        val exception = assertThrows<PubSubException> {
            pubSubLifecycleManager.onStart(mockStartupEvent)
        }

        verify(exactly = 1) {
            mockPubSubSubscriber.startSubscription("application.test.topic-hello.pubsub")
        }

        // Verify the same exception is re-thrown
        assert(exception === expectedException)
    }

    @Test
    fun `onStart should throw exception when general error occurs`() {
        // Given
        val expectedException = RuntimeException("General startup error")
        every { mockPubSubSubscriber.startSubscription(any()) } throws expectedException

        // When & Then
        val exception = assertThrows<RuntimeException> {
            pubSubLifecycleManager.onStart(mockStartupEvent)
        }

        verify(exactly = 1) {
            mockPubSubSubscriber.startSubscription("application.test.topic-hello.pubsub")
        }

        // Verify the same exception is re-thrown
        assert(exception === expectedException)
    }

    @Test
    fun `onStop should shutdown services successfully`() {
        // Given
        every { mockPubSubSubscriber.stopAllSubscriptions() } just Runs
        every { mockPubSubPublisher.shutdown() } just Runs

        // When
        pubSubLifecycleManager.onStop(mockShutdownEvent)

        // Then
        // Verify order: subscriptions stopped first, then publisher shutdown
        verifyOrder {
            mockPubSubSubscriber.stopAllSubscriptions()
            mockPubSubPublisher.shutdown()
        }

        verify(exactly = 1) { mockPubSubSubscriber.stopAllSubscriptions() }
        verify(exactly = 1) { mockPubSubPublisher.shutdown() }
    }

    @Test
    fun `onStop should handle subscriber exception gracefully`() {
        // Given
        every { mockPubSubSubscriber.stopAllSubscriptions() } throws RuntimeException("Subscriber error")
        every { mockPubSubPublisher.shutdown() } just Runs

        // When - Should not throw exception
        pubSubLifecycleManager.onStop(mockShutdownEvent)

        // Then
        verify(exactly = 1) { mockPubSubSubscriber.stopAllSubscriptions() }
        // Publisher shutdown won't be called because the exception is thrown before it
        verify(exactly = 0) { mockPubSubPublisher.shutdown() }
    }

    @Test
    fun `onStop should handle publisher exception gracefully`() {
        // Given
        every { mockPubSubSubscriber.stopAllSubscriptions() } just Runs
        every { mockPubSubPublisher.shutdown() } throws RuntimeException("Publisher error")

        // When - Should not throw exception
        pubSubLifecycleManager.onStop(mockShutdownEvent)

        // Then
        verify(exactly = 1) { mockPubSubSubscriber.stopAllSubscriptions() }
        verify(exactly = 1) { mockPubSubPublisher.shutdown() }
    }

    @Test
    fun `onStop should handle both subscriber and publisher exceptions gracefully`() {
        // Given
        every { mockPubSubSubscriber.stopAllSubscriptions() } throws RuntimeException("Subscriber error")
        every { mockPubSubPublisher.shutdown() } throws RuntimeException("Publisher error")

        // When - Should not throw exception
        pubSubLifecycleManager.onStop(mockShutdownEvent)

        // Then
        verify(exactly = 1) { mockPubSubSubscriber.stopAllSubscriptions() }
        // Publisher shutdown won't be called because subscriber error is thrown first
        verify(exactly = 0) { mockPubSubPublisher.shutdown() }
    }

    @Test
    fun `onStop should not continue with publisher shutdown when subscriber fails`() {
        // Given
        every { mockPubSubSubscriber.stopAllSubscriptions() } throws RuntimeException("Subscriber error")
        every { mockPubSubPublisher.shutdown() } just Runs

        // When
        pubSubLifecycleManager.onStop(mockShutdownEvent)

        // Then - Publisher shutdown should NOT be called when subscriber error occurs
        verify(exactly = 1) { mockPubSubSubscriber.stopAllSubscriptions() }
        verify(exactly = 0) { mockPubSubPublisher.shutdown() }
    }

    @Test
    fun `startAsyncJobSubscriptions should start correct subscription`() {
        // Given
        every { mockPubSubSubscriber.startSubscription(any()) } just Runs

        // When
        pubSubLifecycleManager.onStart(mockStartupEvent)

        // Then
        verify(exactly = 1) {
            mockPubSubSubscriber.startSubscription("application.test.topic-hello.pubsub")
        }
    }

    @Test
    fun `onStart should not start subscriptions twice on multiple calls`() {
        // Given
        every { mockPubSubSubscriber.startSubscription(any()) } just Runs

        // When
        pubSubLifecycleManager.onStart(mockStartupEvent)
        pubSubLifecycleManager.onStart(mockStartupEvent)

        // Then - Should be called twice if onStart is called twice
        verify(exactly = 2) {
            mockPubSubSubscriber.startSubscription("application.test.topic-hello.pubsub")
        }
    }

    @Test
    fun `onStart logs startup information correctly`() {
        // Given
        every { mockPubSubSubscriber.startSubscription(any()) } just Runs

        // When
        pubSubLifecycleManager.onStart(mockStartupEvent)

        // Then - Should complete without throwing
        verify(exactly = 1) {
            mockPubSubSubscriber.startSubscription("application.test.topic-hello.pubsub")
        }
    }

    @Test
    fun `onStop logs shutdown information correctly`() {
        // Given
        every { mockPubSubSubscriber.stopAllSubscriptions() } just Runs
        every { mockPubSubPublisher.shutdown() } just Runs

        // When
        pubSubLifecycleManager.onStop(mockShutdownEvent)

        // Then - Should complete without throwing
        verify(exactly = 1) { mockPubSubSubscriber.stopAllSubscriptions() }
        verify(exactly = 1) { mockPubSubPublisher.shutdown() }
    }

    @Test
    fun `onStart with custom exception types should propagate correctly`() {
        // Given
        val illegalArgumentException = IllegalArgumentException("Invalid subscription name")
        every { mockPubSubSubscriber.startSubscription(any()) } throws illegalArgumentException

        // When & Then
        val exception = assertThrows<IllegalArgumentException> {
            pubSubLifecycleManager.onStart(mockStartupEvent)
        }

        assert(exception === illegalArgumentException)
        verify(exactly = 1) {
            mockPubSubSubscriber.startSubscription("application.test.topic-hello.pubsub")
        }
    }

    @Test
    fun `multiple startup and shutdown cycles should work correctly`() {
        // Given
        every { mockPubSubSubscriber.startSubscription(any()) } just Runs
        every { mockPubSubSubscriber.stopAllSubscriptions() } just Runs
        every { mockPubSubPublisher.shutdown() } just Runs

        // When - Multiple startup/shutdown cycles
        pubSubLifecycleManager.onStart(mockStartupEvent)
        pubSubLifecycleManager.onStop(mockShutdownEvent)
        pubSubLifecycleManager.onStart(mockStartupEvent)
        pubSubLifecycleManager.onStop(mockShutdownEvent)

        // Then
        verify(exactly = 2) {
            mockPubSubSubscriber.startSubscription("application.test.topic-hello.pubsub")
        }
        verify(exactly = 2) { mockPubSubSubscriber.stopAllSubscriptions() }
        verify(exactly = 2) { mockPubSubPublisher.shutdown() }
    }

    @Test
    fun `onStart should fail fast when startAsyncJobSubscriptions throws PubSubException`() {
        // Given
        val pubSubException = PubSubException("Subscription failed")
        every { mockPubSubSubscriber.startSubscription(any()) } throws pubSubException

        // When & Then
        val exception = assertThrows<PubSubException> {
            pubSubLifecycleManager.onStart(mockStartupEvent)
        }

        // Verify exception is immediately re-thrown
        assert(exception === pubSubException)
        verify(exactly = 1) {
            mockPubSubSubscriber.startSubscription("application.test.topic-hello.pubsub")
        }
    }

    @Test
    fun `onStart should fail fast when startAsyncJobSubscriptions throws any Exception`() {
        // Given
        val runtimeException = IllegalStateException("Invalid state")
        every { mockPubSubSubscriber.startSubscription(any()) } throws runtimeException

        // When & Then
        val exception = assertThrows<IllegalStateException> {
            pubSubLifecycleManager.onStart(mockStartupEvent)
        }

        // Verify exception is immediately re-thrown
        assert(exception === runtimeException)
        verify(exactly = 1) {
            mockPubSubSubscriber.startSubscription("application.test.topic-hello.pubsub")
        }
    }
}
