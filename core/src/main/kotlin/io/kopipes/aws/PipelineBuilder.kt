package io.kopipes.aws

import io.kopipes.aws.flavors.Pipeline

/**
 * Base class for pipeline builders designed for Java interoperability.
 */
abstract class PipelineBuilder<T : Pipeline, B : PipelineBuilder<T, B>> {
    protected var id: String? = null

    @Suppress("UNCHECKED_CAST")
    fun id(id: String): B {
        this.id = id
        return this as B
    }

    abstract fun build(): T
}
