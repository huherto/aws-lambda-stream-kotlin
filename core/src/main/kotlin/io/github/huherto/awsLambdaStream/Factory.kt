package io.github.huherto.awsLambdaStream

interface Factory<T> {
    fun create(): T
}
