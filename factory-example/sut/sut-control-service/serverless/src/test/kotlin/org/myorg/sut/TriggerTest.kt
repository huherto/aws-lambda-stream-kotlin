package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.from.DynamodbAdapter
import io.github.huherto.awsLambdaStream.testsupport.TestContext
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.mockk.mockk
import io.mockk.spyk

class TriggerTest : FunSpec({

    context("Trigger internal pipeline initialization") {
        test("should lazily initialize container") {
            // Arrange
            val envConfig = spyk<EnvironmentConfig>()
            val dynamoDbClient = mockk<DynamoDbClient>(relaxed = true)
            val container = TriggerContainer(envConfig, dynamoDbClient)

            val trigger = Trigger(container)

            // Act
            val assembler = container.assembler
            val dynamoDBAdapter = container.dynamoDbAdapter
            val faultManager = container.faultManager

            // Assert
            assembler shouldNotBe null
            assembler.shouldBeInstanceOf<PipelineAssembler>()

            assembler.getFaultManager() shouldNotBe null
            assembler.getFaultManager() shouldBe faultManager

            dynamoDBAdapter shouldNotBe null
            dynamoDBAdapter.shouldBeInstanceOf<DynamodbAdapter>()

        }
    }

    context("Trigger handleRequest") {
        test("should process DynamodbEvent successfully and return 'Done' for various record scenarios") {
            // Arrange
            val envConfig = spyk<EnvironmentConfig>()
            val dynamoDbClient = mockk<DynamoDbClient>(relaxed = true)
            val container = TriggerContainer(envConfig, dynamoDbClient)
            val trigger = Trigger(container)
            val testContext = TestContext()

            val emptyEvent = DynamodbEvent().apply { 
                records = emptyList() 
            }

            val singleRecordEvent = DynamodbEvent().apply {
                records = listOf(
                    DynamodbEvent.DynamodbStreamRecord().apply {
                        eventName = "INSERT"
                        dynamodb = StreamRecord().apply {
                            keys = emptyMap()
                            newImage = emptyMap()
                            oldImage = emptyMap()
                        }
                    }
                )
            }

            // Act
            val emptyEventResult = trigger.handleRequest(emptyEvent, testContext)
            val singleRecordResult = trigger.handleRequest(singleRecordEvent, testContext)

            // Assert
            emptyEventResult shouldBe "Done"
            singleRecordResult shouldBe "Done"
        }
    }
})