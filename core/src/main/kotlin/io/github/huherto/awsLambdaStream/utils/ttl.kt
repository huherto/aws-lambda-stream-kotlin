package io.github.huherto.awsLambdaStream.utils

import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

fun ttl(baseTime: Long, ttlDuration: Duration) : Long {
    return baseTime + ttlDuration.inWholeMilliseconds
}

@OptIn(ExperimentalTime::class)
fun ttl(baseTime: Instant, ttlDuration: Duration) : Instant {
    return baseTime + ttlDuration
}
