package org.myorg.sut

import io.kopipes.aws.EnvironmentConfig
import io.kopipes.aws.GlobalRegistry
import io.kopipes.aws.PipelineAssembler
import io.kopipes.aws.connectors.S3ClientFactory
import io.kopipes.aws.faults.FaultManager
import io.kopipes.aws.flavors.Pipeline
import io.kopipes.aws.from.DynamodbAdapter
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
        val s3ClientFactory: S3ClientFactory = mockk(relaxed = true)
        val envConfig = mockEnvConfig()
        GlobalRegistry.setEnvConfig(envConfig)
        GlobalRegistry.setFaultManager(faultManager)
        GlobalRegistry.setS3ClientFactory(s3ClientFactory)

        val container = DynamoDbTriggerContainer()

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
        val s3ClientFactory : S3ClientFactory = mockk(relaxed = true)
        GlobalRegistry.setEnvConfig(envConfig)
        GlobalRegistry.setS3ClientFactory(s3ClientFactory)
        val container = DynamoDbTriggerContainer()

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