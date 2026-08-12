package io.github.huherto.awsLambdaStream.metrics

data class PipelineMetrics(
    val pipeline: String = "default",
    val timer: Timer = Timer(),
    val gauges: Map<String, List<Double>> = emptyMap()
) {
    fun gauge(key: String, value: Double): PipelineMetrics {
        val k = "$pipeline|$key"
        val currentValues = gauges[k] ?: emptyList()
        return copy(
            gauges = gauges + (k to (currentValues + value))
        )
    }

    fun gauge(key: String, values: List<Double>): PipelineMetrics {
        val k = "$pipeline|$key"
        val currentValues = gauges[k] ?: emptyList()
        return copy(
            gauges = gauges + (k to (currentValues + values))
        )
    }

    fun startPipeline(pipeline: String): PipelineMetrics {
        val newTimer = timer.checkpoint("$pipeline|stream.channel.wait.time")
        return copy(
            pipeline = pipeline,
            timer = newTimer
        )
    }

    fun endPipeline(): PipelineMetrics {
        return copy(
            timer = timer.end("$pipeline|stream.pipeline.time")
        )
    }

    fun startStep(step: String): PipelineMetrics {
        return copy(
            timer = timer.checkpoint("$pipeline|$step|stream.pipeline.io.wait.time")
        )
    }

    fun endStep(step: String): PipelineMetrics {
        return copy(
            timer = timer.checkpoint("$pipeline|$step|stream.pipeline.io.time")
        )
    }
}
