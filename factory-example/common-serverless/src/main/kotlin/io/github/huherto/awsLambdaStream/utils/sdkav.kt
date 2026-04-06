package io.github.huherto.awsLambdaStream.utils

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue as SdkAV

fun nullableS(s: String?): SdkAV {
    return s?.let { SdkAV.S(it) } ?: SdkAV.Null(true)
}

fun nullableN(s: String?): SdkAV {
    return s?.let { SdkAV.N(it) } ?: SdkAV.Null(true)
}

fun nullableBool(s: Boolean?): SdkAV {
    return s?.let { SdkAV.Bool(it) } ?: SdkAV.Null(true)
}