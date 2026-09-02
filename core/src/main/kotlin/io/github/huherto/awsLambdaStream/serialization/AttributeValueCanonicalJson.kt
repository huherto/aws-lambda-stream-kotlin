package io.github.huherto.awsLambdaStream.serialization

import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.*
import java.nio.ByteBuffer
import java.util.*

fun AttributeValue.toCanonicalJsonObject(): JsonObject = buildJsonObject {
    when {
        s != null -> put("S", s)
        n != null -> put("N", n)
        bool != null -> put("BOOL", bool)
        m != null -> put("M", JsonObject(m.mapValues { (_, value) -> value.toCanonicalJsonObjectOrNull() }))
        l != null -> put("L", JsonArray(l.map { it.toCanonicalJsonObjectOrNull() }))
        ss != null -> put("SS", JsonArray(ss.map { JsonPrimitive(it) }))
        ns != null -> put("NS", JsonArray(ns.map { JsonPrimitive(it) }))
        b != null -> put("B", b.encodeBase64())
        bs != null -> put("BS", JsonArray(bs.map { JsonPrimitive(it.encodeBase64()) }))
        else -> put("NULL", true)
    }
}

fun Map<String, AttributeValue?>.toCanonicalJsonObject(): JsonObject =
    JsonObject(mapValues { (_, value) -> value.toCanonicalJsonObjectOrNull() })

fun JsonObject.toCanonicalAttributeValue(): AttributeValue {
    val entry = entries.firstOrNull() ?: return AttributeValue().withNULL(true)
    val (tag, value) = entry

    return when (tag) {
        "S" -> AttributeValue().withS(value.jsonPrimitive.content)
        "N" -> AttributeValue().withN(value.jsonPrimitive.content)
        "BOOL" -> AttributeValue().withBOOL(value.jsonPrimitive.boolean)
        "NULL" -> AttributeValue().withNULL(value.jsonPrimitive.boolean)
        "M" -> AttributeValue().withM(value.jsonObject.mapValues { (_, v) -> v.jsonObject.toCanonicalAttributeValue() })
        "L" -> AttributeValue().withL(value.jsonArray.map { it.jsonObject.toCanonicalAttributeValue() })
        "SS" -> AttributeValue().withSS(value.jsonArray.map { it.jsonPrimitive.content })
        "NS" -> AttributeValue().withNS(value.jsonArray.map { it.jsonPrimitive.content })
        "B" -> AttributeValue().withB(value.jsonPrimitive.content.decodeBase64())
        "BS" -> AttributeValue().withBS(value.jsonArray.map { it.jsonPrimitive.content.decodeBase64() })
        else -> throw SerializationException("Unknown DynamoDB AttributeValue type tag: $tag")
    }
}

fun JsonObject.toCanonicalAttributeValueMap(): Map<String, AttributeValue> =
    mapValues { (_, value) -> value.jsonObject.toCanonicalAttributeValue() }

private fun AttributeValue?.toCanonicalJsonObjectOrNull(): JsonObject =
    this?.toCanonicalJsonObject() ?: buildJsonObject { put("NULL", true) }

private fun ByteBuffer.encodeBase64(): String {
    val bytes = ByteArray(remaining())
    duplicate().get(bytes)
    return Base64.getEncoder().encodeToString(bytes)
}

private fun String.decodeBase64(): ByteBuffer =
    ByteBuffer.wrap(Base64.getDecoder().decode(this))
