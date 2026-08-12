package io.github.huherto.awsLambdaStream.metrics

data class MetricStats(
    val average: Double,
    val min: Double,
    val max: Double,
    val sum: Double,
    val count: Int
)
