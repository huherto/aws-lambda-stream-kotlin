package io.github.huherto.awsLambdaStream

import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.kotest.matchers.shouldBe
import io.mockk.spyk
import kotlinx.coroutines.flow.Flow
import org.junit.jupiter.api.Test


class JsonUtilsTest  {


    @Test
    fun testPipelineSerializer() {

        val envConfig = spyk<EnvironmentConfig>()
        class SomePipeline : Pipeline("pipe1", envConfig) {
            override fun connect(
                fm: FaultManager,
                fromFlow: Flow<UnitOfWork>
            ): Flow<UnitOfWork> {
                TODO("Not yet implemented")
            }
        }

        val pipeline = SomePipeline()
        val pipelineAsString = pipeline.asJson()
        pipelineAsString.replace("\n", "") shouldBe "{  \"id\" : \"pipe1\"}"

    }
}
