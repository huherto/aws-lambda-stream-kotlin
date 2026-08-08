package io.github.huherto.awsLambdaStream.serialization

interface Snapshottable {
    fun toSnapshot(): Any?
}
