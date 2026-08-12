package io.github.huherto.awsLambdaStream.metrics

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

class CalculateMetricsTest {
    @Test
    fun `should calculate stats correctly`() {
        val values = listOf(10.0, 20.0, 30.0)
        val stats = CalculateMetrics.calculateStats(values)

        stats.average shouldBe 20.0
        stats.min shouldBe 10.0
        stats.max shouldBe 30.0
        stats.sum shouldBe 60.0
        stats.count shouldBe 3
    }

    @Test
    fun `should calculate metrics for a batch`() {
        val uow1 = UnitOfWork().withMetrics(
            PipelineMetrics(pipeline = "p1")
                .gauge("g1", 10.0)
                .let { pm ->
                    pm.copy(timer = pm.timer.copy(checkpoints = mapOf("p1|stream.pipeline.time" to 100L)))
                }
        )
        val uow2 = UnitOfWork().withMetrics(
            PipelineMetrics(pipeline = "p1")
                .gauge("g1", 20.0)
                .let { pm ->
                    pm.copy(timer = pm.timer.copy(checkpoints = mapOf("p1|stream.pipeline.time" to 200L)))
                }
        )

        val metrics = CalculateMetrics.calculateMetrics(listOf(uow1, uow2))

        metrics["stream.uow.count"] shouldBe 2.0

        val g1Stats = metrics["p1|g1"] as MetricStats
        g1Stats.average shouldBe 15.0
        g1Stats.count shouldBe 2

        val timeStats = metrics["p1|stream.pipeline.time"] as MetricStats
        timeStats.average shouldBe 150.0

        metrics["p1|stream.pipeline.utilization"] shouldBe 1.0
    }

    @Test
    fun `should collect metrics from flow`() = runBlocking {
        val envConfig = object : EnvironmentConfig() {
            override fun getProperty(name: String): String? = when(name) {
                "METRICS" -> "emf"
                else -> null
            }
        }

        val uow1 = UnitOfWork().withMetrics(PipelineMetrics(pipeline = "p1").gauge("g1", 10.0))
        val flow = flowOf(uow1)

        val results = flow.collectMetrics(envConfig).toList()

        results.size shouldBe 1
        results[0] shouldBe uow1
    }
}
