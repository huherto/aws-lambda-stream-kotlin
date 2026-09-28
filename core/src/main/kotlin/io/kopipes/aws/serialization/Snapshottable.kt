package io.kopipes.aws.serialization

interface Snapshottable {
    fun toSnapshot(): Any?
}
