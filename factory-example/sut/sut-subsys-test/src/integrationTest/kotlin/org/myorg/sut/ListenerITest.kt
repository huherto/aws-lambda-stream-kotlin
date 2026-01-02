package org.myorg.sut

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.kinesis.model.PutRecordRequest
import aws.sdk.kotlin.services.lambda.LambdaClient
import aws.sdk.kotlin.services.lambda.model.CreateFunctionRequest
import aws.sdk.kotlin.services.lambda.model.FunctionCode
import aws.smithy.kotlin.runtime.net.url.Url
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import io.github.huherto.`aws-lambda-stream`.testsupport.KinesisHelper
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.io.File
import java.net.URI


// Components tested.
//   - Connection from Kinesis Stream to Lambda Listener.
//   - Listener being able to write to Dynamodb.
//  Possibly also but not sure
//    - Trigger lambda consuming from Dynamodb stream.
//    - Trigger sending messages to EventBridge.
@Testcontainers
class ListenerITest {
    @Container
    val localstack: LocalStackContainer = LocalStackContainer(DockerImageName.parse("localstack/localstack:stable"))
        .withServices(LocalStackContainer.Service.KINESIS)
        .withExposedPorts(4566)

    @Test
    fun sendEvents(): Unit {
        runBlocking<Unit> {

            val streamName = "my-kinesis-stream"
            val kinesisClient = KinesisHelper.createKinesisClient(localstack)
            val lambdaClient = createLambdaClient(localstack)
            kinesisClient.use { kinesis ->
                KinesisHelper.createKinesisStream(kinesis, streamName)
                val payload = """{"event":"create","id":"abc123"}"""
                val resp = kinesis.putRecord(
                    PutRecordRequest {
                        this.streamName = streamName
                        this.partitionKey = "user-42"
                        this.data = payload.encodeToByteArray()
                    }
                )

                println("PutRecord shardId=${resp.shardId} sequenceNumber=${resp.sequenceNumber}")
            }
        }
    }

    public fun createLambdaClient(localstack : LocalStackContainer): LambdaClient {
        val endpoint = localstack.getEndpoint().toString()
        return LambdaClient {
            this.region = localstack.region
            this.endpointUrl = Url.parse(endpoint)
            credentialsProvider =
                StaticCredentialsProvider {
                    this.accessKeyId = localstack.accessKey
                    this.secretAccessKey = localstack.secretKey
                }

        }
    }

    suspend fun createLambdaFunction(lambdaClient : LambdaClient): URI {
        val functionName = "Listener"
        val roleArn = "arn:aws:iam::000000000000:role/lambda-role" // LocalStack IAM is simple
        val functionZip = File("./build/libs/serverless.jar")
        val functionRequest = CreateFunctionRequest {
            this.functionName = functionName
            this.runtime = aws.sdk.kotlin.services.lambda.model.Runtime.Java21
            this.role = roleArn
            this.handler = "org.myorg.sut.Listener::handleRequest"
            this.code = FunctionCode {
                this.zipFile = functionZip.readBytes()
            }
        }
        val resp = lambdaClient.createFunction(functionRequest)
    }


}