package io.github.huherto.awsLambdaStream.metrics

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

class EmfReporterTest {
    @Test
    fun `should format metrics into EMF structure`() {
        val envConfig = object : EnvironmentConfig() {
            override fun metrics() : String {
                return "test-ns"
            }
        }

        val metrics = mapOf(
            "stream.uow.count" to 2.0,
            "p1|stream.pipeline.time" to MetricStats(150.0, 100.0, 200.0, 300.0, 2),
            "p1|s1|stream.pipeline.io.time" to MetricStats(50.0, 40.0, 60.0, 100.0, 2)
        )

        val entries = EmfReporter.formatMetrics(metrics)

        entries.size shouldBe 3 // function level, p1 level, p1|s1 level

        val functionEntry = entries.find { it.containsKey("stream.uow.count") }!!
        functionEntry["stream.uow.count"] shouldBe 2.0

        val pipelineEntry = entries.find { it["pipeline"] == "p1" && it["step"] == null }!!
        pipelineEntry["stream.pipeline.time.average"] shouldBe 150.0

        val stepEntry = entries.find { it["step"] == "s1" }!!
        stepEntry["stream.pipeline.io.time.average"] shouldBe 50.0
    }
}
