package com.axuanhogan.pubsub.job

import java.util.*

data class HelloPubSubJob(
    override val id: String = UUID.randomUUID().toString(),
    override val topicName: String = "application.test.topic",
    override val correlationId: String? = null,
    val helloId: String,
) : AsyncJob()
