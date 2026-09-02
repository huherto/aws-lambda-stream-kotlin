package io.github.huherto.awsLambdaStream.connectors

import java.util.concurrent.ConcurrentHashMap

/** Interface for creating and managing AWS clients. */
interface ClientFactory<out T> {
    fun getClient(pipelineId: String): T
}

/** Base implementation for [ClientFactory] with client caching. */
abstract class AbstractClientFactory<T> : ClientFactory<T> {

    private val clients = ConcurrentHashMap<String, T>()

    override fun getClient(pipelineId: String): T {
        return clients.computeIfAbsent(pipelineId) {
            create()
        }
    }

    internal fun clearClients() {
        clients.clear()
    }

    protected abstract fun create(): T
}