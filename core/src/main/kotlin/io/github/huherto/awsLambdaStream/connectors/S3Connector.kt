package io.github.huherto.awsLambdaStream.connectors

import aws.sdk.kotlin.runtime.auth.credentials.EnvironmentCredentialsProvider
import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.*
import aws.smithy.kotlin.runtime.content.ByteStream
import aws.smithy.kotlin.runtime.content.toByteArray
import aws.smithy.kotlin.runtime.net.url.Url
import io.github.huherto.awsLambdaStream.GlobalRegistry
import io.github.huherto.awsLambdaStream.GlobalRegistry.envConfig
import io.github.huherto.awsLambdaStream.UnitOfWork

interface S3ClientFactory : ClientFactory<S3Client>

class DefaultS3ClientFactory() : S3ClientFactory, AbstractClientFactory<S3Client>() {
    override fun create(): S3Client {
        val endpointUrl = envConfig().endPointUrl()
        val region = envConfig().awsRegion()

        return S3Client {
            this.region = region
            this.credentialsProvider = EnvironmentCredentialsProvider()
            endpointUrl?.let { this.endpointUrl = Url.parse(it) }
        }
    }
}

/**
 * S3Connector provides a wrapper around the AWS S3 client with debugging support
 * and pipeline-aware client management.
 *
 * @property options Configuration options for the connector.
 */
class S3Connector(private val options: S3Connector.Options) {

    /**
     * Configuration options for [S3Connector].
     *
     * @property clientFactory The factory used to create [S3Client] instances.
     * @property debug A callback for debugging responses and errors.
     */
    data class Options(
        val clientFactory: S3ClientFactory = GlobalRegistry.s3ClientFactory(),
        val debug: (Any?) -> Unit = {},
    )

    fun getClient(uow: UnitOfWork): S3Client {
        val pipelineId = uow.pipeline?.id ?: "unknown"
        return options.clientFactory.getClient(pipelineId)
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
            options.debug(response)
            response
        } catch (error: Throwable) {
            options.debug(error)
            throw error
        }
    }
}
