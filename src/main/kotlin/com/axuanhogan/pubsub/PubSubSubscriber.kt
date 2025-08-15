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
import io.quarkus.logging.Log
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Instance
import jakarta.inject.Inject
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap

@ApplicationScoped
class PubSubSubscriber {

    @Inject
    lateinit var pubSubConfig: PubSubConfig

    @Inject
    lateinit var messageHandlers: Instance<PubSubMessageHandler>

    private val subscribers = ConcurrentHashMap<String, Subscriber>()
    private val coroutineScope = CoroutineScope(Dispatchers.IO)

    fun startSubscription(subscriptionName: String) {
        if (subscribers.containsKey(subscriptionName)) {
            Log.warnf("Subscription %s is already running", subscriptionName)
            return
        }

        try {
            val fullSubscriptionName = ProjectSubscriptionName.of(
                pubSubConfig.projectId,
                subscriptionName
            )

            val receiver = MessageReceiver { message, consumer ->
                coroutineScope.launch {
                    handleMessage(message, consumer)
                }
            }

            val subscriber = Subscriber.newBuilder(fullSubscriptionName, receiver)
                .build()

            subscribers[subscriptionName] = subscriber
            subscriber.startAsync()

            Log.infof("Started subscription: %s", subscriptionName)

        } catch (e: Exception) {
            Log.errorf(e, "Failed to start subscription: %s", subscriptionName)
            throw PubSubException("Failed to start subscription: $subscriptionName", e)
        }
    }

    fun stopSubscription(subscriptionName: String) {
        subscribers[subscriptionName]?.let { subscriber ->
            try {
                subscriber.stopAsync()
                // Wait for subscriber to terminate with timeout (30 seconds)
                try {
                    subscriber.awaitTerminated(30, java.util.concurrent.TimeUnit.SECONDS)
                } catch (e: java.util.concurrent.TimeoutException) {
                    Log.warnf("Subscription %s did not terminate within timeout", subscriptionName)
                } catch (e: Exception) {
                    Log.warnf(e, "Error waiting for subscription %s to terminate", subscriptionName)
                }
                subscribers.remove(subscriptionName)
                Log.infof("Stopped subscription: %s", subscriptionName)
            } catch (e: Exception) {
                Log.errorf(e, "Error stopping subscription: %s", subscriptionName)
            }
        }
    }

    fun stopAllSubscriptions() {
        subscribers.keys.forEach { subscriptionName ->
            stopSubscription(subscriptionName)
        }
    }

    private suspend fun handleMessage(message: PubsubMessage, consumer: AckReplyConsumer) {
        val messageJob = message.attributesMap["messageJob"] ?: "Unknown"
        val messageId = message.messageId

        Log.infof("Received PubSub message ID: %s, Job: %s", messageId, messageJob)

        try {
            val handler = findHandler(messageJob)
            if (handler == null) {
                Log.warnf("No handler found for job: %s, PubSub message ID: %s", messageJob, messageId)
                consumer.ack()
                return
            }

            val result = handler.handle(message)

            when (result) {
                PubSubMessageResult.SUCCESS -> {
                    Log.infof("Successfully processed PubSub message ID: %s", messageId)
                    consumer.ack()
                }
                PubSubMessageResult.RETRY -> {
                    Log.warnf("Message processing failed, will retry. PubSub message ID: %s", messageId)
                    consumer.nack()
                }
                PubSubMessageResult.DEAD_LETTER -> {
                    Log.errorf("Message processing failed permanently, sending to dead letter. PubSub message ID: %s", messageId)
                    consumer.ack() // Ack to prevent retry, message will go to dead letter topic
                }
            }

        } catch (e: Exception) {
            Log.errorf(e, "Unexpected error processing PubSub message ID: %s", messageId)
            consumer.nack()
        }
    }

    private fun findHandler(messageJob: String): PubSubMessageHandler? {
        return messageHandlers.find { handler -> handler.canHandle(messageJob) }
    }
}
