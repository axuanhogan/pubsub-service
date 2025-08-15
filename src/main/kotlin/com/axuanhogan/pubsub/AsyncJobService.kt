package com.axuanhogan.pubsub

import com.axuanhogan.pubsub.job.AsyncJob
import io.quarkus.logging.Log
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject

@ApplicationScoped
class AsyncJobService {

    @Inject
    lateinit var pubSubPublisher: PubSubPublisher

    suspend fun scheduleJob(job: AsyncJob, delaySeconds: Long = 0): String {
        val attributes = mutableMapOf<String, String>()

        if (delaySeconds > 0) {
            attributes["delaySeconds"] = delaySeconds.toString()
        }

        job.correlationId?.let { correlationId ->
            attributes["correlationId"] = correlationId
        }

        Log.infof("Scheduling async Job: %s with jobId: %s", job::class.simpleName, job.id)

        return pubSubPublisher.publishAsync(
            topicName = job.topicName,
            message = job,
            attributes = attributes,
            orderingKey = job.correlationId
        )
    }

    fun scheduleJobFireAndForget(job: AsyncJob, delaySeconds: Long = 0) {
        try {
            val attributes = mutableMapOf<String, String>()

            if (delaySeconds > 0) {
                attributes["delaySeconds"] = delaySeconds.toString()
            }

            job.correlationId?.let { correlationId ->
                attributes["correlationId"] = correlationId
            }

            Log.infof("Scheduling async Job (fire-and-forget): %s with job ID: %s", job::class.simpleName, job.id)

            pubSubPublisher.publishFireAndForget(
                topicName = job.topicName,
                message = job,
                attributes = attributes,
                orderingKey = job.correlationId
            )
        } catch (e: Exception) {
            Log.errorf(e, "Failed to schedule fire-and-forget job: %s", job::class.simpleName)
            // Don't propagate exception for fire-and-forget operations
        }
    }
}
