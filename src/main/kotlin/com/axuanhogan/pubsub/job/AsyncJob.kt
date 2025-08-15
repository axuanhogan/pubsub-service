package com.axuanhogan.pubsub.job

abstract class AsyncJob {
    abstract val id: String
    abstract val topicName: String
    abstract val correlationId: String?
}
