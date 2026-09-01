package io.github.huherto.awsLambdaStream.java

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.GlobalRegistry
import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.faults.FaultManager
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.sinks.EventPublisherInMemory
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.function.Consumer
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue

class PipelineRunnerTest {

    @BeforeEach
    fun beforeEach() {
        GlobalRegistry.reset()
        GlobalRegistry.setEnvConfig(object : EnvironmentConfig() {
            override fun isMetricEnabled(name: String): Boolean = false
        })
        GlobalRegistry.setFaultManager(FaultManager(eventPublisher = EventPublisherInMemory()))
    }

    private class PassThroughPipeline(id: String) : Pipeline(id) {
        override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> {
            return fromFlow
        }
    }

    private fun assembler(): PipelineAssembler {
        return PipelineAssembler.builder()
            .faultManager(FaultManager(eventPublisher = EventPublisherInMemory()))
            .addPipeline(PassThroughPipeline("test-pipeline"))
            .build()
    }

    @Test
    fun `run should require headFlow`() {
        val runner = PipelineRunner<String>(assembler())

        val exception = assertThrows<IllegalArgumentException> {
            runner.run("input")
        }

        assertEquals("headFlowCreator is required", exception.message)
    }

    @Test
    fun `run should create head flow from input and collect assembled flow`() {
        val processed = mutableListOf<UnitOfWork>()

        PipelineRunner<String>(assembler())
            .headFlow { input -> flowOf(UnitOfWork(key = input)) }
            .onEach(Consumer { uow -> processed.add(uow) })
            .run("input-key")

        assertEquals(1, processed.size)
        assertEquals("input-key", processed.single().key)
        assertEquals("test-pipeline", processed.single().pipeline?.id)
    }

    @Test
    fun `run should apply transformer before onEach consumer`() {
        val processedKeys = mutableListOf<String?>()

        PipelineRunner<String>(assembler())
            .headFlow { input -> flowOf(UnitOfWork(key = input)) }
            .transformer { flow ->
                flow.map { uow -> uow.copy(key = "${uow.key}-transformed") }
            }
            .onEach(Consumer { uow -> processedKeys.add(uow.key) })
            .run("input-key")

        processedKeys shouldBe listOf("input-key-transformed")
    }

    @Test
    fun `run should execute multiple onEach consumers in registration order`() {
        val calls = mutableListOf<String>()

        PipelineRunner<String>(assembler())
            .headFlow { input -> flowOf(UnitOfWork(key = input)) }
            .onEach(Consumer { uow -> calls.add("first:${uow.key}") })
            .onEach(Consumer { uow -> calls.add("second:${uow.key}") })
            .run("input-key")

        assertEquals(
            listOf(
                "first:input-key",
                "second:input-key",
            ),
            calls,
        )
    }

    @Test
    fun `builder methods should return same runner instance`() {
        val runner = PipelineRunner<String>(assembler())

        val afterHeadFlow = runner.headFlow { input -> flowOf(UnitOfWork(key = input)) }
        val afterTransformer = runner.transformer { flow -> flow }
        val afterOnEach = runner.onEach(Consumer { })

        assertSame(runner, afterHeadFlow)
        assertSame(runner, afterTransformer)
        assertSame(runner, afterOnEach)
    }

    @Test
    fun `run should collect every emitted unit of work`() {
        val processedKeys = mutableListOf<String?>()

        PipelineRunner<String>(assembler())
            .headFlow {
                flowOf(
                    UnitOfWork(key = "first"),
                    UnitOfWork(key = "second"),
                    UnitOfWork(key = "third"),
                )
            }
            .onEach(Consumer { uow -> processedKeys.add(uow.key) })
            .run("ignored")

        processedKeys shouldBe listOf("first", "second", "third")
    }

    @Test
    fun `run should support multiple transformers in registration order`() {
        val processedKeys = mutableListOf<String?>()

        PipelineRunner<String>(assembler())
            .headFlow { input -> flowOf(UnitOfWork(key = input)) }
            .transformer { flow ->
                flow.map { uow -> uow.copy(key = "${uow.key}-one") }
            }
            .transformer { flow ->
                flow.map { uow -> uow.copy(key = "${uow.key}-two") }
            }
            .onEach(Consumer { uow -> processedKeys.add(uow.key) })
            .run("input")

        processedKeys shouldBe listOf("input-one-two")
    }

    @Test
    fun `run should do nothing when head flow is empty`() {
        var processed = false

        PipelineRunner<String>(assembler())
            .headFlow { flowOf() }
            .onEach(Consumer { processed = true })
            .run("input")

        assertTrue(!processed)
    }
}