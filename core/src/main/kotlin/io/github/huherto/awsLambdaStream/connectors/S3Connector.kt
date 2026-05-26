package io.github.huherto.awsLambdaStream.connectors

import aws.sdk.kotlin.runtime.auth.credentials.EnvironmentCredentialsProvider
import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.*
import aws.smithy.kotlin.runtime.content.ByteStream
import aws.smithy.kotlin.runtime.content.toByteArray
import aws.smithy.kotlin.runtime.net.url.Url
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.copyS3
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map

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




/**
 * Kotlin equivalent of:
 *
 * export const getObjectFromS3 = (...) => (s) => s.map(...).parallel(...)
 *
 * In Kotlin Flow, concurrency can be added by a custom flatMapMerge-based helper if needed.
 */
fun Flow<UnitOfWork>.getObjectFromS3(
    s3Connector: S3Connector,
): Flow<UnitOfWork> = this.map { uow ->
    val request = uow.s3.getRequest ?: return@map uow

    val response = s3Connector.getObject(request, uow)

    uow.copyS3{ copy(getResponse = response) }
}

/**
 * Kotlin equivalent of getObjectFromS3AsByteArray.
 */
fun Flow<UnitOfWork>.getObjectFromS3AsByteArray(
    s3Connector: S3Connector,
): Flow<UnitOfWork> = this.map { uow ->
    val request = uow.s3.getRequest ?: return@map uow

    val response = s3Connector.getObjectAsByteArray(request, uow)

    uow.copyS3 { copy(getResponseBytes = response) }
}

/**
 * Kotlin equivalent of getObjectFromS3AsStream.
 *
 * The TypeScript version emits one UnitOfWork per delimited line/chunk.
 * This Kotlin version reads the object as text, splits it, filters it,
 * and emits one UnitOfWork per matching part.
 */
fun Flow<UnitOfWork>.getObjectFromS3AsStream(
    s3Connector: S3Connector,
    delimiter: String = "\n",
    splitFilter: suspend (String) -> Boolean = { true },
): Flow<UnitOfWork> = this.flatMapConcat { uow ->
    flow {
        val request = uow.s3.getRequest

        if (request == null) {
            emit(uow)
            return@flow
        }

        val text = s3Connector.getObjectAsText(request, uow)

        text
            .split(delimiter)
            .filter { splitFilter(it) }
            .forEach { line ->
                emit(uow.copyS3{copy(getResponseText = line)})
            }
    }
}

/**
 * Kotlin equivalent of splitS3Object.
 *
 * This expects getResponseBytes to already contain the S3 object body.
 */
fun Flow<UnitOfWork>.splitS3Object(
    delimiter: String = "\n",
): Flow<UnitOfWork> = this.flatMapConcat { uow ->
    flow {
        val body = uow.s3.getResponseBytes

        if (body == null) {
            emit(uow)
            return@flow
        }

        body
            .decodeToString()
            .split(delimiter)
            .filter { it.isNotEmpty() }
            .forEach { line ->
                emit(uow.copyS3{ copy(getResponseText = line)})
            }
    }
}

/**
 * Kotlin equivalent of listObjectsFromS3.
 */
fun Flow<UnitOfWork>.listObjectsFromS3(
    s3Connector: S3Connector,
): Flow<UnitOfWork> = this.map { uow ->
    val request = uow.s3.listRequest ?: return@map uow

    val response = s3Connector.listObjects(request, uow)

    uow.copyS3{ copy(listResponse = response) }
}

/**
 * Kotlin equivalent of pageObjectsFromS3.
 *
 * Emits one UnitOfWork per S3 object across all pages.
 */
fun Flow<UnitOfWork>.pageObjectsFromS3(
    s3Connector: S3Connector,
    debug: (Any?) -> Unit = {},
): Flow<UnitOfWork> = this.flatMapConcat { uow ->
    flow {
        val baseRequest = uow.s3.listRequest

        if (baseRequest == null) {
            emit(uow)
            return@flow
        }

        var continuationToken = baseRequest.continuationToken

        do {
            val request = baseRequest.copy {
                this.continuationToken = continuationToken
            }

            val response = s3Connector.listObjects(request, uow)

            debug(
                mapOf(
                    "isTruncated" to response.isTruncated,
                    "nextContinuationToken" to response.nextContinuationToken,
                    "keyCount" to response.keyCount,
                    "name" to response.name,
                    "prefix" to response.prefix,
                )
            )

            response.contents?.forEach { obj ->
                emit(
                    uow.copyS3 {
                        copy(
                            listRequest = request,
                            listResponse = response,
                            listResponseObject = obj,
                        )
                    }
                )
            }

            continuationToken =
                if (response.isTruncated == true) {
                    response.nextContinuationToken
                } else {
                    null
                }
        } while (continuationToken != null)
    }
}

/**
 * Kotlin equivalent of headS3Object.
 */
fun Flow<UnitOfWork>.headS3Object(
    s3Connector: S3Connector,
): Flow<UnitOfWork> = this.map { uow ->
    val request = uow.s3.headRequest ?: return@map uow

    val response = s3Connector.headObject(request, uow)

    uow.copyS3{ copy(headResponse = response) }
}

// ------------------------------------------------------------------------
// Request generators
// ------------------------------------------------------------------------

/**
 * Kotlin equivalent of toGetObjectRequest, but typed.
 *
 * Because the original TypeScript reads from a loosely typed S3 event:
 *
 * uow.record.s3.bucket.name
 * uow.record.s3.object.key
 *
 * this Kotlin version accepts extractor lambdas.
 */
fun toGetObjectRequest(
    uow: UnitOfWork,
    bucketName: (UnitOfWork) -> String,
    objectKey: (UnitOfWork) -> String,
): UnitOfWork {
    return uow.copyS3 {
        copy(
            getRequest = GetObjectRequest {
                bucket = bucketName(uow)
                key = objectKey(uow)
            }
        )
    }
}

/**
 * Convenience overload when bucket/key are already known.
 */
fun toGetObjectRequest(
    uow: UnitOfWork,
    bucketName: String,
    objectKey: String,
): UnitOfWork {
    return uow.copyS3 {
        copy(
            getRequest = GetObjectRequest {
                bucket = bucketName
                key = objectKey
            }
        )
    }
}

/**
 * Helper for creating list requests.
 */
fun toListObjectsRequest(
    uow: UnitOfWork,
    bucketName: String,
    prefix: String? = null,
): UnitOfWork {
    return uow.copyS3 {
        copy(
            listRequest = ListObjectsV2Request {
                bucket = bucketName
                this.prefix = prefix
            }
        )
    }
}