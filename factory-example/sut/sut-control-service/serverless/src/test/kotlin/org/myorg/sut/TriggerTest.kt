package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.flavors.CorrelatePipeline
import io.github.huherto.awsLambdaStream.flavors.EvaluatePipeline
import io.github.huherto.awsLambdaStream.testsupport.TestContext
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.mockk.mockk
import io.mockk.spyk
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.jvm.isAccessible

class TriggerTest : FunSpec({

    context("Trigger internal pipeline initialization") {
        test("should lazily initialize correlatePipeline and evaluatePipeline correctly") {
            // Arrange
            val envConfig = spyk<EnvironmentConfig>()
            val dynamoDbClient = mockk<DynamoDbClient>(relaxed = true)
            val trigger = Trigger(envConfig, dynamoDbClient)

            val correlatePipelineProp = Trigger::class.declaredMemberProperties.find { it.name == "correlatePipeline" }
            correlatePipelineProp?.isAccessible = true

            val evaluatePipelineProp = Trigger::class.declaredMemberProperties.find { it.name == "evaluatePipeline" }
            evaluatePipelineProp?.isAccessible = true

            // Act
            val correlatePipeline = correlatePipelineProp?.get(trigger)
            val evaluatePipeline = evaluatePipelineProp?.get(trigger)

            // Assert
            correlatePipeline shouldNotBe null
            correlatePipeline.shouldBeInstanceOf<CorrelatePipeline>()

            evaluatePipeline shouldNotBe null
            evaluatePipeline.shouldBeInstanceOf<EvaluatePipeline>()
        }
    }

    context("Trigger handleRequest") {
        test("should process DynamodbEvent successfully and return 'Done' for various record scenarios") {
            // Arrange
            val envConfig = spyk<EnvironmentConfig>()
            val dynamoDbClient = mockk<DynamoDbClient>(relaxed = true)
            val trigger = Trigger(envConfig, dynamoDbClient)
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