package io.github.huherto.awsLambdaStream

import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.flow.Flow
import org.junit.jupiter.api.Test


class JsonUtilsTest  {


    @Test
    fun testPipelineSerializer() {

        class SomePipeline : Pipeline("pipe1") {
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
