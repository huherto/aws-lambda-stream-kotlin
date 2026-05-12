package io.github.huherto.awsLambdaStream.utils

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue as SdkAV

fun nullableS(s: String?): SdkAV {
    return s?.let { SdkAV.S(it) } ?: SdkAV.Null(true)
}

fun nullableN(s: String?): SdkAV {
    return s?.let { SdkAV.N(it) } ?: SdkAV.Null(true)
}

fun nullableN(value: Double?) : SdkAV {
    if (value == null) return SdkAV.Null(true)
    return SdkAV.N(value.toString())
}

fun nullableBool(s: Boolean?): SdkAV {
    return s?.let { SdkAV.Bool(it) } ?: SdkAV.Null(true)
}