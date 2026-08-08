package io.github.huherto.awsLambdaStream.extensions

import aws.sdk.kotlin.services.s3.model.*
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.serialization.Snapshottable

data class S3UnitOfWork(
    val getRequest: GetObjectRequest? = null,
    val getResponse: GetObjectResponse? = null,
    val deleteRequest: DeleteObjectRequest? = null,
    val deleteResponse: DeleteObjectResponse? = null,
    val copyRequest: CopyObjectRequest? = null,
    val copyResponse: CopyObjectResponse? = null,
    val getResponseText: String? = null,
    val getResponseBytes: ByteArray? = null,
    val putRequest: PutObjectRequest? = null,
    val putResponse: PutObjectResponse? = null,
    val listRequest: ListObjectsV2Request? = null,
    val listResponse: ListObjectsV2Response? = null,
    val listResponseObject: Object? = null,
    val headRequest: HeadObjectRequest? = null,
    val headResponse: HeadObjectResponse? = null,
) : Snapshottable {
    override fun toSnapshot(): Any {
        return mapOf(
            "getRequest" to getRequest?.toString(),
            "getResponse" to getResponse?.toString(),
            "deleteRequest" to deleteRequest?.toString(),
            "deleteResponse" to deleteResponse?.toString(),
            "copyRequest" to copyRequest?.toString(),
            "copyResponse" to copyResponse?.toString(),
            "getResponseText" to getResponseText,
            "getResponseBytes" to getResponseBytes,
            "putRequest" to putRequest?.toString(),
            "putResponse" to putResponse?.toString(),
            "listRequest" to listRequest?.toString(),
            "listResponse" to listResponse?.toString(),
            "listResponseObject" to listResponseObject?.toString(),
            "headRequest" to headRequest?.toString(),
            "headResponse" to headResponse?.toString(),
        ).filterValues { it != null }
    }
    fun isEmpty(): Boolean =
        getRequest == null &&
            getResponse == null &&
            deleteRequest == null &&
            deleteResponse == null &&
            copyRequest == null &&
            copyResponse == null &&
            getResponseText == null &&
            getResponseBytes == null &&
            putRequest == null &&
            putResponse == null &&
            listRequest == null &&
            listResponse == null &&
            listResponseObject == null &&
            headRequest == null &&
            headResponse == null

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as S3UnitOfWork

        if (getRequest != other.getRequest) return false
        if (getResponse != other.getResponse) return false
        if (deleteRequest != other.deleteRequest) return false
        if (deleteResponse != other.deleteResponse) return false
        if (copyRequest != other.copyRequest) return false
        if (copyResponse != other.copyResponse) return false
        if (getResponseText != other.getResponseText) return false
        if (!getResponseBytes.contentEquals(other.getResponseBytes)) return false
        if (putRequest != other.putRequest) return false
        if (putResponse != other.putResponse) return false
        if (listRequest != other.listRequest) return false
        if (listResponse != other.listResponse) return false
        if (listResponseObject != other.listResponseObject) return false
        if (headRequest != other.headRequest) return false
        if (headResponse != other.headResponse) return false

        return true
    }

    override fun hashCode(): Int {
        var result = getRequest?.hashCode() ?: 0
        result = 31 * result + (getResponse?.hashCode() ?: 0)
        result = 31 * result + (deleteRequest?.hashCode() ?: 0)
        result = 31 * result + (deleteResponse?.hashCode() ?: 0)
        result = 31 * result + (copyRequest?.hashCode() ?: 0)
        result = 31 * result + (copyResponse?.hashCode() ?: 0)
        result = 31 * result + (getResponseText?.hashCode() ?: 0)
        result = 31 * result + (getResponseBytes?.contentHashCode() ?: 0)
        result = 31 * result + (putRequest?.hashCode() ?: 0)
        result = 31 * result + (putResponse?.hashCode() ?: 0)
        result = 31 * result + (listRequest?.hashCode() ?: 0)
        result = 31 * result + (listResponse?.hashCode() ?: 0)
        result = 31 * result + (listResponseObject?.hashCode() ?: 0)
        result = 31 * result + (headRequest?.hashCode() ?: 0)
        result = 31 * result + (headResponse?.hashCode() ?: 0)
        return result
    }
}

val UnitOfWork.s3: S3UnitOfWork
    get() = getExtension() ?: S3UnitOfWork()

fun UnitOfWork.copyS3(
    transform: S3UnitOfWork.() -> S3UnitOfWork,
): UnitOfWork {
    val updated = s3.transform()
    return withExtension(updated)
}
