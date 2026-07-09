package org.myorg.sut

import aws.sdk.kotlin.services.cloudwatch.CloudWatchClient
import aws.sdk.kotlin.services.cloudwatch.model.DescribeAlarmsRequest
import aws.sdk.kotlin.services.route53.Route53Client
import aws.sdk.kotlin.services.route53.model.HealthCheck
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.myorg.sut.facades.*
import kotlin.time.Duration.Companion.milliseconds

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RegionalHealthCheckITest {

    private val logger = KotlinLogging.logger {}
    private val awsRegion = "us-east-1"
    private val serviceName = "sut-regional-health-check"
    private val stageName = "local"
    private val tracerTableName = "sut-regional-health-check-local-tracer"
    private val bucketName = "myorg-sut-regional-health-check-local-us-east-1"
    private val streamName = "sut-regional-health-check-local-s1"
    private val apigAlarmName = "$serviceName-$stageName-Apig5xxAlarm"
    private val dynamoDbAlarmName = "$serviceName-$stageName-DynamoDB5xxAlarm"
    private val regionalHealthAlarmName = "$serviceName-$stageName-$awsRegion"
    private val checkHealthApiFacade = CheckHealthApiFacade()
    private val s3Facade = S3Facade()
    private val dynamoDbFacade = DynamoDbFacade(tracerTableName)
    private val kinesisFacade = KinesisFacade(streamName = streamName)
    private val config = AwsLocalConfig()
    private val cloudWatchClient = CloudWatchClient {
        region = config.region
        endpointUrl = config.endpointUrl
        credentialsProvider = config.credentialsProvider()
    }
    private val route53Client = Route53Client {
        region = config.region
        endpointUrl = config.endpointUrl
        credentialsProvider = config.credentialsProvider()
    }

    @Test
    fun verifyRegionalHealthCheckInfrastructure(): Unit = runBlocking {
        // Arrange / Act
        val metricAlarmsResponse = cloudWatchClient.describeAlarms(
            DescribeAlarmsRequest {
               //alarmNames = listOf(apigAlarmName, dynamoDbAlarmName)
            }
        )

        val compositeAlarmsResponse = cloudWatchClient.describeAlarms(
            DescribeAlarmsRequest {
                alarmNames = listOf(regionalHealthAlarmName)
            }
        )

        val healthChecks = waitForHealthChecks(
            apigAlarmName = apigAlarmName,
            dynamoDbAlarmName = dynamoDbAlarmName,
        )

        // Assert CloudWatch metric alarms
        val apigAlarm = metricAlarmsResponse.metricAlarms
            .orEmpty()
            .firstOrNull { it.alarmName == apigAlarmName }

        apigAlarm.shouldNotBeNull()
        apigAlarm.namespace shouldBe "AWS/ApiGateway"
        apigAlarm.metricName shouldBe "5XXError"
        apigAlarm.comparisonOperator?.value shouldBe "GreaterThanThreshold"
        apigAlarm.threshold shouldBe 0.0
        apigAlarm.period shouldBe 60
        apigAlarm.evaluationPeriods shouldBe 5
        apigAlarm.statistic?.value shouldBe "Sum"
        apigAlarm.treatMissingData shouldBe "notBreaching"
        apigAlarm.dimensions
            .orEmpty()
            .firstOrNull { it.name == "ApiName" }
            ?.value shouldBe "$stageName-$serviceName"

        val dynamoDbAlarm = metricAlarmsResponse.metricAlarms
            .orEmpty()
            .firstOrNull { it.alarmName == dynamoDbAlarmName }

        dynamoDbAlarm.shouldNotBeNull()
        dynamoDbAlarm.namespace shouldBe "AWS/DynamoDB"
        dynamoDbAlarm.metricName shouldBe "SystemErrors"
        dynamoDbAlarm.comparisonOperator?.value shouldBe "GreaterThanThreshold"
        dynamoDbAlarm.threshold shouldBe 0.0
        dynamoDbAlarm.period shouldBe 60
        dynamoDbAlarm.evaluationPeriods shouldBe 5
        dynamoDbAlarm.statistic?.value shouldBe "Sum"
        dynamoDbAlarm.treatMissingData shouldBe "notBreaching"
        dynamoDbAlarm.dimensions
            .orEmpty()
            .firstOrNull { it.name == "TableName" }
            ?.value shouldBe "$serviceName-$stageName-entities"

        // Assert composite alarm
        val regionalCompositeAlarm = compositeAlarmsResponse.compositeAlarms
            .orEmpty()
            .firstOrNull { it.alarmName == regionalHealthAlarmName }

        regionalCompositeAlarm.shouldNotBeNull()
        regionalCompositeAlarm.alarmRule shouldBe "ALARM($apigAlarmName) OR ALARM($dynamoDbAlarmName)"

        val apigHealthCheck = healthChecks.singleCloudWatchMetricHealthCheckByCallerReference(
            callerReferencePrefix = "ApigHealthCheck-",
        )
        val dynamoDbHealthCheck = healthChecks.singleCloudWatchMetricHealthCheckByCallerReference(
            callerReferencePrefix = "DynamoDBHealthCheck-",
        )
        val regionalHealthCheck = healthChecks.singleCalculatedHealthCheckReferencing(
            apigHealthCheck.id.shouldNotBeNull(),
            dynamoDbHealthCheck.id.shouldNotBeNull(),
        )

        regionalHealthCheck.healthCheckConfig?.healthThreshold shouldBe 2
        regionalHealthCheck.healthCheckConfig
            ?.childHealthChecks
            .orEmpty()
            .shouldContainAll(
                apigHealthCheck.id.shouldNotBeNull(),
                dynamoDbHealthCheck.id.shouldNotBeNull(),
            )
    }

    @Test
    fun verifyCheckHealthTriggersTraceActions(): Unit = runBlocking {
        // Arrange

        // Act
        val response = checkHealthApiFacade.check()

        // Assert
        verifyTracerReachesDynamoDbTable(response)

        verifyTracerReachesS3(response)

        verifyTracerReachesKinesisStream(response)

        verifyTracerIsComplete(response)

        val startTime = System.currentTimeMillis()
        while(true) {
            val latestResponse = checkHealthApiFacade.check()
            if (latestResponse.statusCode == 200) {
                break
            }
            val elapsedTime = System.currentTimeMillis() - startTime
            if (elapsedTime > 10_000) {
                throw RuntimeException("Timed out waiting for checkHealth to return 200.")
            }
        }

    }
    private suspend fun verifyTracerReachesDynamoDbTable(response: HealthCheckResponse) {
        // Assert
        val savedRecord = dynamoDbFacade.findEntityByPK(awsRegion) { items ->
            items?.firstOrNull { item ->
                item["sk"]?.asS() == response.timestamp.toString()
            }
        }

        savedRecord.shouldNotBeNull()
        savedRecord["pk"]?.asS() shouldBe awsRegion
        savedRecord["sk"]?.asS() shouldBe response.timestamp.toString()
        savedRecord["status"]?.asS() shouldBe "STARTED"
        savedRecord["discriminator"]?.asS() shouldBe "trace"
        savedRecord["awsregion"]?.asS() shouldBe awsRegion
        savedRecord["timestamp"]?.asN().shouldNotBeNull()
        savedRecord["ttl"]?.asN().shouldNotBeNull()
    }


    private suspend fun verifyTracerReachesS3(response: HealthCheckResponse) {
        val expectedKey = "$awsRegion/${response.timestamp}"

        val content = s3Facade.getObjectWithKey(bucketName, expectedKey)

        content.shouldNotBeNull()
        content.contains(awsRegion) shouldBe true
        content.contains(response.timestamp.toString()) shouldBe true
        content.contains("STARTED") shouldBe true
    }

    private suspend fun verifyTracerReachesKinesisStream(response: HealthCheckResponse) {
        val matchingRecord: String? = kinesisFacade.waitForResult(
            onTimeout = {
                logger.error { "Timed out waiting for kinesis record." }
                null
            },
            block = {
                val records = kinesisFacade.readAllEvents()
                records
                    .onEach { record -> logger.info { "Kinesis record: $record" } }
                    .firstOrNull { record ->
                        record.contains(awsRegion) &&
                                record.contains(response.timestamp.toString()) &&
                                record.contains("STARTED")
                    }
            },
        )

        matchingRecord.shouldNotBeNull()
        matchingRecord.contains(awsRegion) shouldBe true
        matchingRecord.contains(response.timestamp.toString()) shouldBe true
        matchingRecord.contains("STARTED") shouldBe true
    }

    private suspend fun verifyTracerIsComplete(response: HealthCheckResponse) {
        // Verify DynamoDB has COMPLETE status
        val completedRecord = dynamoDbFacade.findEntityByPK(awsRegion) { items ->
            items?.firstOrNull { item ->
                val sk = item["sk"]?.asS()
                val status = item["status"]?.asS()
                sk == response.timestamp.toString() && status == "COMPLETED"
            }
        }
        completedRecord.shouldNotBeNull()
        completedRecord["status"]?.asS() shouldBe "COMPLETED"

    }

    private suspend fun waitForHealthChecks(
        apigAlarmName: String,
        dynamoDbAlarmName: String,
        timeoutMillis: Long = 30_000,
        pollIntervalMillis: Long = 1_000,
    ): List<HealthCheck> {
        val startTime = System.currentTimeMillis()
        var latestHealthChecks: List<HealthCheck> = emptyList()

        while (System.currentTimeMillis() - startTime < timeoutMillis) {
            latestHealthChecks = route53Client.listHealthChecks()
                .healthChecks
                .orEmpty()

            val hasApigHealthCheck = latestHealthChecks.hasCloudWatchMetricHealthCheckByCallerReference(
                callerReferencePrefix = "ApigHealthCheck-",
            )
            val hasDynamoDbHealthCheck = latestHealthChecks.hasCloudWatchMetricHealthCheckByCallerReference(
                callerReferencePrefix = "DynamoDBHealthCheck-",
            )

            if (hasApigHealthCheck && hasDynamoDbHealthCheck) {
                return latestHealthChecks
            }

            logger.info {
                "Waiting for Route53 health checks. " +
                        "Expected CloudWatch metric health checks for alarms: [$apigAlarmName, $dynamoDbAlarmName]. " +
                        "Found health checks: ${latestHealthChecks.describeForDebugging()}"
            }

            delay(pollIntervalMillis.milliseconds)
        }

        throw AssertionError(
            "Timed out waiting for Route53 CLOUDWATCH_METRIC health checks. " +
                    "Expected CloudWatch metric health checks for alarms: [$apigAlarmName, $dynamoDbAlarmName]. " +
                    "Found health checks: ${latestHealthChecks.describeForDebugging()}"
        )
    }

    private fun List<HealthCheck>.hasCloudWatchMetricHealthCheckByCallerReference(
        callerReferencePrefix: String,
    ): Boolean {
        return any { healthCheck ->
            val config = healthCheck.healthCheckConfig
            config?.type?.value == "CLOUDWATCH_METRIC" &&
                    healthCheck.callerReference?.startsWith(callerReferencePrefix) == true
        }
    }

    private fun List<HealthCheck>.describeForDebugging(): String {
        return joinToString(
            prefix = "[",
            postfix = "]",
        ) { healthCheck ->
            val config = healthCheck.healthCheckConfig
            "HealthCheck(id=${healthCheck.id}, " +
                    "callerReference=${healthCheck.callerReference}, " +
                    "type=${config?.type?.value}, " +
                    "alarmName=${config?.alarmIdentifier?.name}, " +
                    "alarmRegion=${config?.alarmIdentifier?.region?.value}, " +
                    "healthThreshold=${config?.healthThreshold}, " +
                    "childHealthChecks=${config?.childHealthChecks})"
        }
    }

    private fun List<HealthCheck>.singleCloudWatchMetricHealthCheckByCallerReference(
        callerReferencePrefix: String,
    ): HealthCheck {
        return singleOrNull { healthCheck ->
            val config = healthCheck.healthCheckConfig
            config?.type?.value == "CLOUDWATCH_METRIC" &&
                    healthCheck.callerReference?.startsWith(callerReferencePrefix) == true
        }.shouldNotBeNull()
    }

    private fun List<HealthCheck>.singleCalculatedHealthCheckReferencing(
        firstChildHealthCheckId: String,
        secondChildHealthCheckId: String,
    ): HealthCheck {
        return singleOrNull { healthCheck ->
            val config = healthCheck.healthCheckConfig
            config?.type?.value == "CALCULATED" &&
                    config.healthThreshold == 2 &&
                    config.childHealthChecks.orEmpty().containsAll(
                        listOf(firstChildHealthCheckId, secondChildHealthCheckId)
                    )
        }.shouldNotBeNull()
    }
    @AfterAll
    fun tearDownAll() {
        s3Facade.close()
        dynamoDbFacade.close()
        kinesisFacade.close()
        cloudWatchClient.close()
        route53Client.close()
    }
}