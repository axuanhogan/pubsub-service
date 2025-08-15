package com.axuanhogan.pubsub.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.pubsub.v1.PubsubMessage
import com.axuanhogan.pubsub.job.HelloPubSubJob
import io.quarkus.logging.Log
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject

@ApplicationScoped
class HelloPubSubJobHandler : PubSubMessageHandler {

    @Inject
    lateinit var objectMapper: ObjectMapper

    override fun canHandle(messageJob: String): Boolean {
        return messageJob == HelloPubSubJob::class.simpleName
    }

    override suspend fun handle(message: PubsubMessage): PubSubMessageResult {
        return try {
            val messageData = message.data.toStringUtf8()
            val job = objectMapper.readValue(messageData, HelloPubSubJob::class.java)

            Log.infof("Processing jobId: %s for helloId: %s",
                job.id, job.helloId)

            // Process the point transaction
            process(job)

            Log.infof("Successfully processed jobId: %s", job.id)
            PubSubMessageResult.SUCCESS

        } catch (e: Exception) {
            Log.errorf(e, "Failed to process message: %s", message.messageId)

            // Determine if this should be retried or sent to dead letter
            // val retryCount = message.attributesMap["retryCount"]?.toIntOrNull() ?: 0
            // if (retryCount < 3) {
            //    PubSubMessageResult.RETRY
            // } else {
                PubSubMessageResult.DEAD_LETTER
            // }
        }
    }

    private suspend fun process(job: HelloPubSubJob) {
        // Add your actual business logic here
        Log.info("[Test] Business logic here, jobId: ${job.id}, helloId: ${job.helloId}")
    }
}
