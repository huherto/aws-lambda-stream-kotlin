package io.github.huherto.awsLambdaStream.metrics

import io.github.huherto.awsLambdaStream.UnitOfWork

object CalculateMetrics {

    fun calculateStats(values: List<Double>): MetricStats {
        if (values.isEmpty()) {
            return MetricStats(0.0, 0.0, 0.0, 0.0, 0)
        }
        val sum = values.sum()
        val count = values.size
        val average = sum / count
        val min = values.minOrNull() ?: 0.0
        val max = values.maxOrNull() ?: 0.0
        return MetricStats(average, min, max, sum, count)
    }

    fun calculateMetrics(
        collected: List<UnitOfWork>,
        functionMetrics: Map<String, Any> = emptyMap()
    ): Map<String, Any> {
        val results = mutableMapOf<String, Any>()

        results.putAll(functionMetrics)
        results["stream.uow.count"] = collected.size.toDouble()

        val allCheckpoints = mutableMapOf<String, MutableList<Double>>()
        collected.forEach { uow ->
            uow.metrics?.timer?.checkpoints?.forEach { (key, value) ->
                allCheckpoints.getOrPut(key) { mutableListOf() }.add(value.toDouble())
            }
        }

        val allGauges = mutableMapOf<String, MutableList<Double>>()
        collected.forEach { uow ->
            uow.metrics?.gauges?.forEach { (key, values) ->
                allGauges.getOrPut(key) { mutableListOf() }.addAll(values)
            }
        }

        val allKeys = allCheckpoints.keys + allGauges.keys
        val stats = allKeys.associateWith { key ->
            val values = (allCheckpoints[key] ?: emptyList<Double>()) + (allGauges[key] ?: emptyList<Double>())
            calculateStats(values)
        }

        results.putAll(stats)

        // Pipeline utilization
        stats.filter { it.key.endsWith("stream.pipeline.time") }.forEach { (key, stat) ->
            val pipeline = key.split("|").firstOrNull() ?: "default"
            results["$pipeline|stream.pipeline.utilization"] = stat.count.toDouble() / collected.size.toDouble()
        }

        return results
    }
}
