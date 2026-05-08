package io.github.huherto.awsLambdaStream.connectors

import java.util.concurrent.ConcurrentHashMap

interface ClientFactory<out T> {
    fun getClient(pipelineId: String): T
}

abstract class AbstractClientFactory<out T> : ClientFactory<T> {

    private val clients = ConcurrentHashMap<String, T >()

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