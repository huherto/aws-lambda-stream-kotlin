package org.myorg.sut.facades

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.smithy.kotlin.runtime.net.url.Url

data class AwsLocalConfig(
    val region: String = "us-east-1",
    val endpointUrl: Url = Url.parse("http://localhost:4566"),
    val accessKeyId: String = "test",
    val secretAccessKey: String = "test",
) {
    fun credentialsProvider(): StaticCredentialsProvider =
        StaticCredentialsProvider {
            this.accessKeyId = this@AwsLocalConfig.accessKeyId
            this.secretAccessKey = this@AwsLocalConfig.secretAccessKey
        }
}