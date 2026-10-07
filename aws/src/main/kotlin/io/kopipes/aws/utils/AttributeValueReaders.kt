package io.kopipes.aws.utils

import io.kopipes.core.utils.AttributeValueMapReader
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue as DynamoDbAttributeValue
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue as StreamAttributeValue


/** Reads from a Stream AttributeValue map. */
class StreamAttributeValueMapReader(private val map: Map<String, StreamAttributeValue?>) : AttributeValueMapReader {

    override fun getS(fieldName: String): String? {
        return map[fieldName]?.s
    }

    override fun getDouble(fieldName: String): Double? {
        return map[fieldName]?.n?.toDouble()
    }

    override fun getInt(fieldName: String): Int? {
        return map[fieldName]?.n?.toInt()
    }

    override fun getBool(fieldName: String): Boolean? {
        return map[fieldName]?.bool
    }

    override fun getLong(fieldName: String): Long? {
        return map[fieldName]?.n?.toLong()
    }

    override fun isNull(fieldName: String): Boolean? {
        return map[fieldName]?.isNULL
    }
}

/** Reads from a DynamoDb AttributeValue map. */
class DynamoDbAttributeValueMapReader(private val map: Map<String, DynamoDbAttributeValue?>) : AttributeValueMapReader {

    override fun getS(fieldName: String): String? {
        return map[fieldName]?.asSOrNull()
    }

    override fun getDouble(fieldName: String): Double? {
        return map[fieldName]?.asNOrNull()?.toDouble()
    }

    override fun getInt(fieldName: String): Int? {
        return map[fieldName]?.asNOrNull()?.toInt()
    }

    override fun getBool(fieldName: String): Boolean? {
        return map[fieldName]?.asBoolOrNull()
    }

    override fun getLong(fieldName: String): Long? {
        return map[fieldName]?.asNOrNull()?.toLong()
    }

    override fun isNull(fieldName: String): Boolean? {
        return map[fieldName]?.asNull()
    }
}