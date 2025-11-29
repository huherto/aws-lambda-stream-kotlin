package org.myorg.sut

data class Event<E>(val id: String) {
    private val type: String? = null
    protected val timestamp: Long = 0
    private val partitionKey: String? = null
    private val tags: Map<String, String>? = null
    private val entity: E? = null
    private val raw: Any? = null
    private val eem: Any? = null
}