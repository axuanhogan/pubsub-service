package com.axuanhogan.pubsub.config

import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.inject.ConfigProperty

@ApplicationScoped
class PubSubConfig {

    @ConfigProperty(name = "quarkus.google.cloud.project-id")
    lateinit var projectId: String

    @ConfigProperty(name = "quarkus.google.cloud.pubsub.enable-ordering", defaultValue = "false")
    var enableOrdering: Boolean = false
}
