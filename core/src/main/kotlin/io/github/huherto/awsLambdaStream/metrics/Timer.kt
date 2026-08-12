package io.github.huherto.awsLambdaStream.metrics

data class Timer(
    val start: Long = System.currentTimeMillis(),
    val last: Long = start,
    val checkpoints: Map<String, Long> = emptyMap()
) {
    fun checkpoint(key: String): Timer {
        val now = now()
        return copy(
            checkpoints = checkpoints + (key to (now - last)),
            last = now
        )
    }

    fun end(key: String): Timer {
        val now = now()
        val firstCheckpointValue = checkpoints.values.firstOrNull() ?: 0L
        return copy(
            checkpoints = checkpoints + (key to (now - (firstCheckpointValue + start))),
            last = now
        )
    }

    companion object {
        var now: () -> Long = { System.currentTimeMillis() }
    }
}
