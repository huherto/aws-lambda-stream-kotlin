package io.github.huherto.awsLambdaStream

import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.sinks.EventPublisherInMemory
import io.mockk.coEvery
import io.mockk.spyk
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class PipelineAssemblerTest {

    // A mock pipeline to test how the assembler routes flows
    class MockPipeline(id: String) : Pipeline(id) {
        override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> {
            return fromFlow.map { it.copy(key = "processed_by_$id") }
        }
    }

    // A mock pipeline that intentionally throws a FailureException to test error handling
    class FailingPipeline(id: String) : Pipeline(id) {
        override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> {
            with(fm) {
                return fromFlow.mapNotNull {
                    faulty(it) {
                        throw FaultException(it, RuntimeException("Intentional failure for $id"))
                    }
                }
            }
        }
    }

    private val envConfig : EnvironmentConfig by lazy {
        val spy = spyk<EnvironmentConfig>()
        coEvery { spy.awsRegion() } returns "us-east-1"
        //coEvery { spy.tableName() } returns "test-table"
        spy
    }

    @Test
    fun `test builder creates assembler with added pipelines`() {
        val pipeline1 = MockPipeline("p1")
        val pipeline2 = MockPipeline("p2")

        val assembler = PipelineAssembler.builder()
            .faultManager(FaultManager(envConfig = envConfig, eventPublisher = EventPublisherInMemory()))
            .addPipeline(pipeline1)
            .addPipeline(pipeline2)
            .build()

        assertNotNull(assembler)
    }

    @Test
    fun `test assemble routes UnitOfWork through all pipelines`() = runBlocking {
        val pipeline1 = MockPipeline("p1")
        val pipeline2 = MockPipeline("p2")

        val assembler = PipelineAssembler.builder()
            .faultManager(FaultManager(envConfig = envConfig, eventPublisher = EventPublisherInMemory()))
            .addPipeline(pipeline1)
            .addPipeline(pipeline2)
            .build()

        // Provide a mock flow
        val headFlow = flowOf(UnitOfWork(record = "test_record"))

        // Disable fault handler to focus on routing and processing 
        val resultFlow = assembler.assemble(headFlow, includeFaultHandler = false)
        val results = resultFlow.toList()

        // It should duplicate the UoW into every pipeline, resulting in 2 outputs
        assertEquals(2, results.size, "Should produce two outputs, one from each pipeline")

        val keys = results.map { it.key }.toSet()
        assertTrue(keys.contains("processed_by_p1"), "Pipeline 1 should have processed an item")
        assertTrue(keys.contains("processed_by_p2"), "Pipeline 2 should have processed an item")

        // Assert that the `pipeline` field was populated correctly on `onEach`
        val p1Result = results.find { it.key == "processed_by_p1" }
        assertEquals(pipeline1, p1Result?.pipeline)

        val p2Result = results.find { it.key == "processed_by_p2" }
        assertEquals(pipeline2, p2Result?.pipeline)
    }

    @Test
    fun `test assemble with includeFaultHandler set to true`() = runBlocking {
        val pipeline1 = MockPipeline("p1")

        val assembler = PipelineAssembler.builder()
            .faultManager(FaultManager(envConfig = envConfig, eventPublisher = EventPublisherInMemory()))
            .addPipeline(pipeline1)
            .build()

        val headFlow = flowOf(UnitOfWork(record = "test_record"))

        // Implicitly checks that catchFailures doesn't break standard flow processing
        val resultFlow = assembler.assemble(headFlow, includeFaultHandler = true)
        val results = resultFlow.toList()

        assertEquals(1, results.size)
        assertEquals("processed_by_p1", results.first().key)
    }


    @Test
    fun `test assemble handles FailureException and publishes FailureEvent`() = runBlocking {
        val failingPipeline = FailingPipeline("fail1")

        val eventPublisher = EventPublisherInMemory()
        val fm = FaultManager(
            envConfig = envConfig,
            eventPublisher = eventPublisher,
        )

        val assembler = PipelineAssembler.builder()
            .addPipeline(failingPipeline)
            .faultManager(fm)
            .build()

        val uow = UnitOfWork(record = "test_record")
        val headFlow = flowOf(uow)

        val resultFlow = assembler.assemble(headFlow, includeFaultHandler = true)
        val results = resultFlow.toList()

        assertEquals(0, results.size, "Flow should be empty due to failure")

        val publishedEvents = eventPublisher.events()
        assertEquals(1, publishedEvents.size, "Should publish one failure event")
        assertEquals(0, fm.getFaults().size, "Faults should be empty after publishing")

        // Retrieve the event from the captured UnitOfWork
        val failureEvent = publishedEvents[0] as? FaultEvent
        assertNotNull(failureEvent)
        assertEquals(FAULT_EVENT_TYPE, failureEvent?.eventType())
        
        val error = failureEvent?.err
        assertNotNull(error)
        assertEquals(uow.record, failureEvent?.uow?.record)
    }

    @Test
    fun `test startPipeline and endPipeline simply return uow`() {
        val assembler = PipelineAssembler.builder()
            .faultManager(FaultManager(envConfig = envConfig, eventPublisher = EventPublisherInMemory()))
            .build()
        val uow = UnitOfWork(key = "test")

        val startedUow = assembler.startPipeline(uow)
        assertEquals(uow, startedUow)

        val endedUow = assembler.endPipeline(uow)
        assertEquals(uow, endedUow)
    }
}