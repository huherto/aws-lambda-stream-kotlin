package org.myorg.sut

import io.kopipes.aws.from.KinesisAdapter
import io.kopipes.core.EnvironmentConfig
import io.kopipes.aws.AwsKopipesContext
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.sinks.EventsMicrostore
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.types.shouldBeInstanceOf
import io.mockk.mockk
import org.junit.jupiter.api.Test

class ListenerContainerTest {

    @Test
    fun `Container properties should be initialized correctly`() {
        // Arrange
        val envConfig: EnvironmentConfig = mockk(relaxed = true)
        val eventsMicrostore: EventsMicrostore = mockk(relaxed = true)
        val faultManager: FaultManager = mockk(relaxed = true)
        val context = AwsKopipesContext(faultManager = faultManager)

        val container = ListenerContainer(
            context,
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