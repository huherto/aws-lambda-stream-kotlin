package io.github.huherto.awsLambdaStream.connectors

import aws.sdk.kotlin.runtime.auth.credentials.EnvironmentCredentialsProvider
import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.*
import aws.smithy.kotlin.runtime.content.ByteStream
import aws.smithy.kotlin.runtime.content.toByteArray
import aws.smithy.kotlin.runtime.net.url.Url
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.UnitOfWork

interface S3ClientFactory : ClientFactory<S3Client>

class DefaultS3ClientFactory(
    private val envConfig: EnvironmentConfig,
) : S3ClientFactory, AbstractClientFactory<S3Client>() {
    override fun create(): S3Client {
        val endpointUrl = envConfig.endPointUrl()
        val region = envConfig.awsRegion()

        return S3Client {
            this.region = region
            this.credentialsProvider = EnvironmentCredentialsProvider()
            endpointUrl?.let { this.endpointUrl = Url.parse(it) }
        }
    }
}

class S3Connector(
    val debug: (Any?) -> Unit = {},
    private val clientFactory: S3ClientFactory,
) {

    fun getClient(uow: UnitOfWork): S3Client {
        val pipelineId = uow.pipeline?.id ?: "unknown"
        return clientFactory.getClient(pipelineId)
    }

    suspend fun getObject(
        getRequest: GetObjectRequest,
        uow: UnitOfWork,
    ): GetObjectResponse {
        val client = getClient(uow)

        return sendCommand {
            client.getObject(getRequest) { response ->
                response
            }
        }
    }

    suspend fun getObjectAsByteArray(
        getRequest: GetObjectRequest,
        uow: UnitOfWork,
    ): ByteArray {
        val client = getClient(uow)

        return sendCommand {
            client.getObject(getRequest) { response ->
                response.body?.toByteArray() ?: ByteArray(0)
            }
        }
    }

    suspend fun getObjectAsText(
        getRequest: GetObjectRequest,
        uow: UnitOfWork,
    ): String {
        return getObjectAsByteArray(getRequest, uow).decodeToString()
    }

    suspend fun getObjectBody(
        getRequest: GetObjectRequest,
        uow: UnitOfWork,
    ): ByteStream? {
        val client = getClient(uow)

        return sendCommand {
            client.getObject(getRequest) { response ->
                response.body
            }
        }
    }

    suspend fun listObjects(
        listRequest: ListObjectsV2Request,
        uow: UnitOfWork,
    ): ListObjectsV2Response {
        val client = getClient(uow)

        return sendCommand {
            client.listObjectsV2(listRequest)
        }
    }

    suspend fun headObject(
        headRequest: HeadObjectRequest,
        uow: UnitOfWork,
    ): HeadObjectResponse {
        val client = getClient(uow)

        return sendCommand {
            client.headObject(headRequest)
        }
    }

    suspend fun putObject(
        putRequest: PutObjectRequest,
        uow: UnitOfWork,
    ): PutObjectResponse {
        val client = getClient(uow)

        return sendCommand {
            client.putObject(putRequest)
        }
    }

    suspend fun deleteObject(
        deleteRequest: DeleteObjectRequest,
        uow: UnitOfWork,
    ): DeleteObjectResponse {
        val client = getClient(uow)

        return sendCommand {
            client.deleteObject(deleteRequest)
        }
    }

    suspend fun copyObject(
        copyRequest: CopyObjectRequest,
        uow: UnitOfWork,
    ): CopyObjectResponse {
        val client = getClient(uow)

        return sendCommand {
            client.copyObject(copyRequest)
        }
    }

    private suspend fun <T> sendCommand(
        block: suspend () -> T,
    ): T {
        return try {
            val response = block()
            debug(response)
            response
        } catch (error: Throwable) {
            debug(error)
            throw error
        }
    }
}
