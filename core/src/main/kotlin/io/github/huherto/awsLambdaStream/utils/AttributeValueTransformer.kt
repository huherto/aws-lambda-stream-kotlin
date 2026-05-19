package io.github.huherto.awsLambdaStream.utils

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue as DynamoDbAttributeValue
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue as StreamAttributeValue

/**
 * Converts a Map of Lambda Event AttributeValues to Kotlin SDK AttributeValues
 */
fun Map<String, StreamAttributeValue>.toSdkMap(): Map<String, DynamoDbAttributeValue> {
    return this.mapValues { (_, value) -> value.toSdkAV() }
}

/**
 * Maps a single Event AttributeValue to a Kotlin SDK AttributeValue recursively
 */
fun StreamAttributeValue.toSdkAV(): DynamoDbAttributeValue {
    return when {
        s != null -> DynamoDbAttributeValue.S(s)
        n != null -> DynamoDbAttributeValue.N(n)
        bool != null -> DynamoDbAttributeValue.Bool(bool)
        m != null -> DynamoDbAttributeValue.M(m.toSdkMap())
        l != null -> DynamoDbAttributeValue.L(l.map { it.toSdkAV() })
        ss != null -> DynamoDbAttributeValue.Ss(ss)
        ns != null -> DynamoDbAttributeValue.Ns(ns)
        bs != null -> DynamoDbAttributeValue.Bs(bs.map { it.array() })
        b != null -> DynamoDbAttributeValue.B(b.array())
        isNULL -> DynamoDbAttributeValue.Null(true)
        else -> throw IllegalArgumentException("Unsupported AttributeValue type")
    }
}

interface AttributeValueMapReader {
    fun getS(fieldName: String) : String?
    fun getDouble(fieldName: String) : Double?
    fun getInt(fieldName: String) : Int?
    fun getBool(fieldName: String) : Boolean?
    fun getLong(fieldName: String) : Long?
    fun isNull(fieldName: String) : Boolean?
}

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
        return map[fieldName]?.n?.toBoolean()
    }

    override fun getLong(fieldName: String): Long? {
        return map[fieldName]?.n?.toLong()
    }

    override fun isNull(fieldName: String): Boolean? {
        return map[fieldName]?.isNULL
    }
}

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