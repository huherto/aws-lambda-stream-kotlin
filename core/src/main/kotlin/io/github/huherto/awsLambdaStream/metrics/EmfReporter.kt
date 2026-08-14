package io.github.huherto.awsLambdaStream.metrics

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.utils.envTags
import mu.KotlinLogging

object EmfReporter {

    private val logger=  KotlinLogging.logger { }

    fun formatUnit(metric: String, key: String? = null): String {
        return when {
            metric.endsWith("count") || key == "count" -> "Count"
            metric.endsWith("bytes") -> "Bytes"
            metric.endsWith("time") -> "Milliseconds"
            metric.endsWith("utilization") -> "Percent"
            else -> "None"
        }
    }


    private data class FormattedMetric(
        val Name: String,
        val Unit: String,
        val value: Any
    )

    private fun formatValue(metric: String, value: Any): List<FormattedMetric> {
        return if (value is MetricStats) {
            listOf(
                FormattedMetric("${metric}.average", formatUnit(metric, "average"), value.average),
                FormattedMetric("${metric}.min", formatUnit(metric, "min"), value.min),
                FormattedMetric("${metric}.max", formatUnit(metric, "max"), value.max),
                FormattedMetric("${metric}.sum", formatUnit(metric, "sum"), value.sum),
                FormattedMetric("${metric}.count", formatUnit(metric, "count"), value.count.toDouble()),
            )
        } else {
            listOf(
                FormattedMetric(metric, formatUnit(metric), value)
            )
        }
    }

    fun formatMetrics(
        metrics: Map<String, Any>,
        envConfig: EnvironmentConfig
    ): List<Map<String, Any>> {
        val timestamp = System.currentTimeMillis()
        val namespace = envConfig.nameSpace() ?: "lambda-stream/metrics"
        val tags = envTags(envConfig, null).toMutableMap()
        tags.remove("pipeline")

        val functionMetrics = mutableListOf<FormattedMetric>()
        val pipelineMetricsMap = mutableMapOf<String, MutableList<FormattedMetric>>()
        val stepMetricsMap = mutableMapOf<String, MutableList<FormattedMetric>>()

        metrics.forEach { (key, value) ->
            val parts = key.split("|")
            val metricName: String
            val pipeline: String?
            val step: String?

            when (parts.size) {
                3 -> {
                    pipeline = parts[0]
                    step = parts[1]
                    metricName = parts[2]
                    val k = "$pipeline|$step"
                    stepMetricsMap.getOrPut(k) { mutableListOf() }.addAll(formatValue(metricName, value))
                }
                2 -> {
                    pipeline = parts[0]
                    metricName = parts[1]
                    pipelineMetricsMap.getOrPut(pipeline) { mutableListOf() }.addAll(formatValue(metricName, value))
                }
                else -> {
                    metricName = parts[0]
                    functionMetrics.addAll(formatValue(metricName, value))
                }
            }
        }

        val entries = mutableListOf<Map<String, Any>>()

        // Function level
        if (functionMetrics.isNotEmpty()) {
            entries.add(createEntry(namespace, timestamp, functionMetrics, tags, FunctionDimensions))
        }

        // Pipeline level
        pipelineMetricsMap.forEach { (pipeline, values) ->
            val pipelineTags = tags + ("pipeline" to pipeline)
            entries.add(createEntry(namespace, timestamp, values, pipelineTags, PipelineDimensions))
        }

        // Step level
        stepMetricsMap.forEach { (key, values) ->
            val kParts = key.split("|")
            val pipeline = kParts[0]
            val step = kParts[1]
            val stepTags = tags + ("pipeline" to pipeline) + ("step" to step)
            entries.add(createEntry(namespace, timestamp, values, stepTags, StepDimensions))
        }

        return entries
    }

    private fun createEntry(
        namespace: String,
        timestamp: Long,
        formattedMetrics: List<FormattedMetric>,
        tags: Map<String, String>,
        dimensions: List<List<String>>
    ): Map<String, Any> {
        val valuesMap = formattedMetrics.associate { it.Name to it.value }
        val metricsList = formattedMetrics.map { mapOf("Name" to it.Name, "Unit" to it.Unit) }

        return tags + valuesMap + mapOf(
            "_aws" to mapOf(
                "Timestamp" to timestamp,
                "CloudWatchMetrics" to listOf(
                    mapOf(
                        "Namespace" to namespace,
                        "Dimensions" to dimensions,
                        "Metrics" to metricsList
                    )
                )
            )
        )
    }

    private val FunctionDimensions = listOf(listOf("functionname", "source", "stage", "region", "account"))
    private val PipelineDimensions = listOf(listOf("pipeline", "functionname", "source", "stage", "region", "account"))
    private val StepDimensions = listOf(listOf("step", "pipeline", "functionname", "source", "stage", "region", "account"))

    fun logMetrics(metrics: Map<String, Any>, envConfig: EnvironmentConfig) {
        if (envConfig.isMetricEnabled("emf")) {
            val emf = formatMetrics(metrics, envConfig)
            emf.forEach { m ->
                logger.info { m }
            }
        } else {
            // Standard logging
            
        }
    }
}
