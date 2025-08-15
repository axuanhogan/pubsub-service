package com.axuanhogan.pubsub

import com.axuanhogan.pubsub.config.PubSubConfig
import com.axuanhogan.pubsub.exception.PubSubException
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.TopicName
import io.quarkus.logging.Log
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.util.concurrent.CompletableFuture

@ApplicationScoped
class PubSubPublisher {

    @Inject
    lateinit var pubSubConfig: PubSubConfig

    @Inject
    lateinit var objectMapper: ObjectMapper

    private val publishers = mutableMapOf<String, Publisher>()

    suspend fun publishAsync(topicName: String, message: Any, attributes: Map<String, String> = emptyMap()): String {
        return publishAsync(topicName, message, attributes, null)
    }

    suspend fun publishAsync(
        topicName: String,
        message: Any,
        attributes: Map<String, String> = emptyMap(),
        orderingKey: String?
    ): String {
        val publisher = getOrCreatePublisher(topicName)
        val messageJson = objectMapper.writeValueAsString(message)

        val pubsubMessageBuilder = PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8(messageJson))
            .putAllAttributes(attributes)
            .putAttributes("messageJob", message::class.simpleName ?: "Unknown")
            .putAttributes("timestamp", System.currentTimeMillis().toString())

        if (orderingKey != null && pubSubConfig.enableOrdering) {
            pubsubMessageBuilder.orderingKey = orderingKey
        }

        val pubsubMessage = pubsubMessageBuilder.build()

        return try {
            val future = publisher.publish(pubsubMessage)
            val messageId = withContext(Dispatchers.IO) {
                future.get()
            }
            Log.infof("Published PubSub message ID: %s to topic: %s", messageId, topicName)
            messageId
        } catch (e: Exception) {
            Log.errorf(e, "Failed to publish message to topic: %s", topicName)
            throw PubSubException("Failed to publish message to topic: $topicName", e)
        }
    }

    fun publishFireAndForget(topicName: String, message: Any, attributes: Map<String, String> = emptyMap()) {
        publishFireAndForget(topicName, message, attributes, null)
    }

    fun publishFireAndForget(
        topicName: String,
        message: Any,
        attributes: Map<String, String> = emptyMap(),
        orderingKey: String?
    ) {
        CompletableFuture.runAsync {
            try {
                val publisher = getOrCreatePublisher(topicName)
                val messageJson = objectMapper.writeValueAsString(message)

                val pubsubMessageBuilder = PubsubMessage.newBuilder()
                    .setData(ByteString.copyFromUtf8(messageJson))
                    .putAllAttributes(attributes)
                    .putAttributes("messageJob", message::class.simpleName ?: "Unknown")
                    .putAttributes("timestamp", System.currentTimeMillis().toString())

                if (orderingKey != null && pubSubConfig.enableOrdering) {
                    pubsubMessageBuilder.orderingKey = orderingKey
                }

                val pubsubMessage = pubsubMessageBuilder.build()
                val future = publisher.publish(pubsubMessage)

                future.addListener({
                    try {
                        val messageId = future.get()
                        Log.infof("Published PubSub message ID: %s to topic: %s", messageId, topicName)
                    } catch (e: Exception) {
                        Log.errorf(e, "Failed to publish message to topic: %s", topicName)
                    }
                }, Runnable::run)

            } catch (e: Exception) {
                Log.errorf(e, "Failed to publish fire-and-forget message to topic: %s", topicName)
            }
        }
    }

    private fun getOrCreatePublisher(topicName: String): Publisher {
        return publishers.computeIfAbsent(topicName) { topic ->
            val fullTopicName = TopicName.of(pubSubConfig.projectId, topic)
            val publisherBuilder = Publisher.newBuilder(fullTopicName)

            if (pubSubConfig.enableOrdering) {
                publisherBuilder.setEnableMessageOrdering(true)
            }

            publisherBuilder.build()
        }
    }

    fun shutdown() {
        publishers.values.forEach { publisher ->
            try {
                // Shutdown publisher gracefully with timeout
                publisher.shutdown()
                try {
                    if (!publisher.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)) {
                        Log.warnf("Publisher did not terminate within timeout")
                    }
                } catch (e: Exception) {
                    Log.warnf(e, "Error waiting for publisher termination")
                }
            } catch (e: Exception) {
                Log.warnf(e, "Error shutting down publisher")
            }
        }
        publishers.clear()
        Log.infof("All PubSub publishers have been shut down")
    }
}
