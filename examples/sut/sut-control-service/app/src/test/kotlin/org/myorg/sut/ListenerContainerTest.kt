package org.myorg.sut

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.GlobalRegistry
import io.github.huherto.awsLambdaStream.faults.FaultManager
import io.github.huherto.awsLambdaStream.from.KinesisAdapter
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.types.shouldBeInstanceOf
import io.mockk.mockk
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class ListenerContainerTest {

    @BeforeEach
    fun setUp() {
        GlobalRegistry.reset()
    }

    @Test
    fun `Container properties should be initialized correctly`() {
        // Arrange
        val envConfig: EnvironmentConfig = mockk(relaxed = true)
        val eventsMicrostore: EventsMicrostore = mockk(relaxed = true)
        val faultManager: FaultManager = mockk(relaxed = true)
        GlobalRegistry.setFaultManager(faultManager)

        val container = ListenerContainer(
            eventsMicrostore = eventsMicrostore,
        )

        // Act & Assert
        val kinesisAdapter = container.kinesisAdapter
        kinesisAdapter.shouldNotBeNull()
        kinesisAdapter.shouldBeInstanceOf<KinesisAdapter>()

        val assembler = container.assembler
        assembler.shouldNotBeNull()
    }

}