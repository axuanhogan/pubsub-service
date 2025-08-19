package com.axuanhogan.pubsub

import com.axuanhogan.pubsub.exception.PubSubException
import com.axuanhogan.pubsub.job.AsyncJob
import io.mockk.*
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.Assertions.assertEquals

class AsyncJobServiceTest {

    private lateinit var asyncJobService: AsyncJobService
    private lateinit var mockPubSubPublisher: PubSubPublisher

    private class TestJob(
        override val id: String,
        override val topicName: String,
        override val correlationId: String?
    ) : AsyncJob()

    @BeforeEach
    fun setup() {
        mockPubSubPublisher = mockk<PubSubPublisher>()
        asyncJobService = AsyncJobService()
        asyncJobService.pubSubPublisher = mockPubSubPublisher
    }

    @Test
    fun `scheduleJob should publish job with basic attributes`() = runBlocking {
        // Given
        val job = TestJob("job-1", "test-topic", null)
        val expectedMessageId = "msg-123"

        coEvery {
            mockPubSubPublisher.publishAsync(
                topicName = "test-topic",
                message = job,
                attributes = emptyMap(),
                orderingKey = null
            )
        } returns expectedMessageId

        // When
        val result = asyncJobService.scheduleJob(job)

        // Then
        assertEquals(expectedMessageId, result)
        coVerify(exactly = 1) {
            mockPubSubPublisher.publishAsync(
                topicName = "test-topic",
                message = job,
                attributes = emptyMap(),
                orderingKey = null
            )
        }
    }

    @Test
    fun `scheduleJob should publish job with delay attributes`() = runBlocking {
        // Given
        val job = TestJob("job-1", "test-topic", null)
        val expectedMessageId = "msg-123"
        val expectedAttributes = mapOf("delaySeconds" to "30")

        coEvery {
            mockPubSubPublisher.publishAsync(
                topicName = "test-topic",
                message = job,
                attributes = expectedAttributes,
                orderingKey = null
            )
        } returns expectedMessageId

        // When
        val result = asyncJobService.scheduleJob(job, 30)

        // Then
        assertEquals(expectedMessageId, result)
        coVerify(exactly = 1) {
            mockPubSubPublisher.publishAsync(
                topicName = "test-topic",
                message = job,
                attributes = expectedAttributes,
                orderingKey = null
            )
        }
    }

    @Test
    fun `scheduleJob should publish job with correlationId`() = runBlocking {
        // Given
        val job = TestJob("job-1", "test-topic", "corr-123")
        val expectedMessageId = "msg-123"
        val expectedAttributes = mapOf("correlationId" to "corr-123")

        coEvery {
            mockPubSubPublisher.publishAsync(
                topicName = "test-topic",
                message = job,
                attributes = expectedAttributes,
                orderingKey = "corr-123"
            )
        } returns expectedMessageId

        // When
        val result = asyncJobService.scheduleJob(job)

        // Then
        assertEquals(expectedMessageId, result)
        coVerify(exactly = 1) {
            mockPubSubPublisher.publishAsync(
                topicName = "test-topic",
                message = job,
                attributes = expectedAttributes,
                orderingKey = "corr-123"
            )
        }
    }

    @Test
    fun `scheduleJob should publish job with delay and correlationId`() = runBlocking {
        // Given
        val job = TestJob("job-1", "test-topic", "corr-123")
        val expectedMessageId = "msg-123"
        val expectedAttributes = mapOf(
            "delaySeconds" to "60",
            "correlationId" to "corr-123"
        )

        coEvery {
            mockPubSubPublisher.publishAsync(
                topicName = "test-topic",
                message = job,
                attributes = expectedAttributes,
                orderingKey = "corr-123"
            )
        } returns expectedMessageId

        // When
        val result = asyncJobService.scheduleJob(job, 60)

        // Then
        assertEquals(expectedMessageId, result)
        coVerify(exactly = 1) {
            mockPubSubPublisher.publishAsync(
                topicName = "test-topic",
                message = job,
                attributes = expectedAttributes,
                orderingKey = "corr-123"
            )
        }
    }

    @Test
    fun `scheduleJob should propagate publisher exception`(): Unit = runBlocking {
        // Given
        val job = TestJob("job-1", "test-topic", null)
        val exception = PubSubException("Publisher error")

        coEvery {
            mockPubSubPublisher.publishAsync(any(), any(), any(), any())
        } throws exception

        // When & Then
        assertThrows<PubSubException> {
            runBlocking { asyncJobService.scheduleJob(job) }
        }
    }

    @Test
    fun `scheduleJobFireAndForget should publish job with basic attributes`() {
        // Given
        val job = TestJob("job-1", "test-topic", null)

        every {
            mockPubSubPublisher.publishFireAndForget(
                topicName = "test-topic",
                message = job,
                attributes = emptyMap(),
                orderingKey = null
            )
        } just Runs

        // When
        asyncJobService.scheduleJobFireAndForget(job)

        // Then
        verify(exactly = 1) {
            mockPubSubPublisher.publishFireAndForget(
                topicName = "test-topic",
                message = job,
                attributes = emptyMap(),
                orderingKey = null
            )
        }
    }

    @Test
    fun `scheduleJobFireAndForget should publish job with delay attributes`() {
        // Given
        val job = TestJob("job-1", "test-topic", null)
        val expectedAttributes = mapOf("delaySeconds" to "45")

        every {
            mockPubSubPublisher.publishFireAndForget(
                topicName = "test-topic",
                message = job,
                attributes = expectedAttributes,
                orderingKey = null
            )
        } just Runs

        // When
        asyncJobService.scheduleJobFireAndForget(job, 45)

        // Then
        verify(exactly = 1) {
            mockPubSubPublisher.publishFireAndForget(
                topicName = "test-topic",
                message = job,
                attributes = expectedAttributes,
                orderingKey = null
            )
        }
    }

    @Test
    fun `scheduleJobFireAndForget should publish job with correlationId`() {
        // Given
        val job = TestJob("job-1", "test-topic", "corr-456")
        val expectedAttributes = mapOf("correlationId" to "corr-456")

        every {
            mockPubSubPublisher.publishFireAndForget(
                topicName = "test-topic",
                message = job,
                attributes = expectedAttributes,
                orderingKey = "corr-456"
            )
        } just Runs

        // When
        asyncJobService.scheduleJobFireAndForget(job)

        // Then
        verify(exactly = 1) {
            mockPubSubPublisher.publishFireAndForget(
                topicName = "test-topic",
                message = job,
                attributes = expectedAttributes,
                orderingKey = "corr-456"
            )
        }
    }

    @Test
    fun `scheduleJobFireAndForget should publish job with delay and correlationId`() {
        // Given
        val job = TestJob("job-1", "test-topic", "corr-789")
        val expectedAttributes = mapOf(
            "delaySeconds" to "90",
            "correlationId" to "corr-789"
        )

        every {
            mockPubSubPublisher.publishFireAndForget(
                topicName = "test-topic",
                message = job,
                attributes = expectedAttributes,
                orderingKey = "corr-789"
            )
        } just Runs

        // When
        asyncJobService.scheduleJobFireAndForget(job, 90)

        // Then
        verify(exactly = 1) {
            mockPubSubPublisher.publishFireAndForget(
                topicName = "test-topic",
                message = job,
                attributes = expectedAttributes,
                orderingKey = "corr-789"
            )
        }
    }

    @Test
    fun `scheduleJobFireAndForget should not propagate publisher exceptions`() {
        // Given
        val job = TestJob("job-1", "test-topic", null)

        every {
            mockPubSubPublisher.publishFireAndForget(any(), any(), any(), any())
        } throws RuntimeException("Publisher error")

        // When & Then - Should not throw exception
        asyncJobService.scheduleJobFireAndForget(job)

        verify(exactly = 1) {
            mockPubSubPublisher.publishFireAndForget(any(), any(), any(), any())
        }
    }
}
