package com.axuanhogan.pubsub.handler

import com.google.pubsub.v1.PubsubMessage

interface PubSubMessageHandler {
    fun canHandle(messageJob: String): Boolean
    suspend fun handle(message: PubsubMessage): PubSubMessageResult
}

enum class PubSubMessageResult {
    SUCCESS,
    RETRY,
    DEAD_LETTER
}
