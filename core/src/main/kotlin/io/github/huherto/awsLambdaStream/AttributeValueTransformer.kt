package io.github.huherto.awsLambdaStream

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue as SdkAV
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue as EventAV

/**
 * Converts a Map of Lambda Event AttributeValues to Kotlin SDK AttributeValues
 */
fun Map<String, EventAV>.toSdkMap(): Map<String, SdkAV> {
    return this.mapValues { (_, value) -> value.toSdkAV() }
}

/**
 * Maps a single Event AttributeValue to a Kotlin SDK AttributeValue recursively
 */
fun EventAV.toSdkAV(): SdkAV {
    return when {
        s != null -> SdkAV.S(s)
        n != null -> SdkAV.N(n)
        bool != null -> SdkAV.Bool(bool)
        m != null -> SdkAV.M(m.toSdkMap())
        l != null -> SdkAV.L(l.map { it.toSdkAV() })
        ss != null -> SdkAV.Ss(ss)
        ns != null -> SdkAV.Ns(ns)
        bs != null -> SdkAV.Bs(bs.map { it.array() })
        b != null -> SdkAV.B(b.array())
        isNULL -> SdkAV.Null(true)
        else -> throw IllegalArgumentException("Unsupported AttributeValue type")
    }
}