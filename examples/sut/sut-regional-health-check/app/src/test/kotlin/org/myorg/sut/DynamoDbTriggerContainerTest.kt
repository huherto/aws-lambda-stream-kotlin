package org.myorg.sut

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.GlobalRegistry
import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.connectors.S3Connector
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.from.DynamodbAdapter
import io.kotest.matchers.types.shouldBeInstanceOf
import io.kotest.matchers.types.shouldBeSameInstanceAs
import io.mockk.coEvery
import io.mockk.mockk
import io.mockk.spyk
import org.junit.jupiter.api.Test
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.jvm.isAccessible

class DynamoDbTriggerContainerTest {

    fun mockEnvConfig() : EnvironmentConfig {
        val envConfig: EnvironmentConfig = spyk()
        coEvery { envConfig.awsRegion() } returns "us-east-1"
        coEvery { envConfig.bucketName() } returns "bucket-name"
        return envConfig
    }

    @Test
    fun `container should initialize adapter and assembler with provided dependencies`() {
        // Arrange
        val faultManager: FaultManager = mockk(relaxed = true)
        val s3Connector: S3Connector = mockk(relaxed = true)
        val envConfig = mockEnvConfig()
        GlobalRegistry.setEnvConfig(envConfig)
        GlobalRegistry.setFaultManager(faultManager)

        val container = DynamoDbTriggerContainer(
            s3Connector = s3Connector,
        )

        // Act
        val dynamoDbAdapter = container.dynamoDbAdapter
        val assembler = container.assembler

        // Assert
        dynamoDbAdapter.shouldBeInstanceOf<DynamodbAdapter>()
        assembler.shouldBeInstanceOf<PipelineAssembler>()
        assembler.getFaultManager().shouldBeSameInstanceAs(faultManager)
    }

    @Test
    fun `lazy properties should return the same instances when accessed repeatedly`() {
        // Arrange
        val envConfig = mockEnvConfig()
        GlobalRegistry.setEnvConfig(envConfig)
        val container = DynamoDbTriggerContainer(
            s3Connector = mockk(relaxed = true),
        )

        // Act
        val firstAssembler = container.assembler
        val secondAssembler = container.assembler
        val firstMaterializeS3Pipeline = container.materializeS3Pipeline()
        val secondMaterializeS3Pipeline = container.materializeS3Pipeline()

        // Assert
        secondAssembler.shouldBeSameInstanceAs(firstAssembler)
        secondMaterializeS3Pipeline.shouldBeSameInstanceAs(firstMaterializeS3Pipeline)
    }

    private fun DynamoDbTriggerContainer.materializeS3Pipeline(): Pipeline {
        val property = DynamoDbTriggerContainer::class
            .declaredMemberProperties
            .single { it.name == "materializeS3Pipeline" }

        property.isAccessible = true

        return property.get(this) as Pipeline
    }
}