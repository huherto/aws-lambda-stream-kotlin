package org.myorg.sut

import io.kopipes.aws.from.KinesisAdapter
import io.kopipes.core.EnvironmentConfig
import io.kopipes.core.GlobalRegistry
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.sinks.EventsMicrostore
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