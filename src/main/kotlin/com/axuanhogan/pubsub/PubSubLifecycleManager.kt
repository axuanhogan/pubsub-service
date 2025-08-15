package com.axuanhogan.pubsub

import io.quarkus.logging.Log
import io.quarkus.runtime.ShutdownEvent
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import jakarta.inject.Inject

@ApplicationScoped
class PubSubLifecycleManager {

    @Inject
    lateinit var pubSubSubscriber: PubSubSubscriber

    @Inject
    lateinit var pubSubPublisher: PubSubPublisher

    fun onStart(@Observes startupEvent: StartupEvent) {
        Log.info("Starting Pub/Sub subscriptions...")

        try {
            // Start subscriptions for async job processing
            startAsyncJobSubscriptions()

            Log.info("Pub/Sub subscriptions started successfully")
        } catch (e: Exception) {
            Log.errorf(e, "Failed to start Pub/Sub subscriptions")
            throw e
        }
    }

    fun onStop(@Observes shutdownEvent: ShutdownEvent) {
        Log.info("Stopping Pub/Sub services...")

        try {
            // Stop all subscriptions first
            pubSubSubscriber.stopAllSubscriptions()

            // Then shutdown all publishers to free gRPC channels
            pubSubPublisher.shutdown()

            Log.info("Pub/Sub services stopped successfully")
        } catch (e: Exception) {
            Log.errorf(e, "Error stopping Pub/Sub services")
        }
    }

    private fun startAsyncJobSubscriptions() {
        // Start subscription for point transaction jobs
        pubSubSubscriber.startSubscription("application.test.topic-hello.pubsub")

        Log.info("Started async job subscriptions: application.test.topic-hello.pubsub")
    }
}
