package org.myorg.sut.facades

import aws.sdk.kotlin.services.lambda.LambdaClient

class LambdaFacade(
    private val config: AwsLocalConfig = AwsLocalConfig(),
) {
    val client: LambdaClient by lazy {
        LambdaClient {
            region = config.region
            endpointUrl = config.endpointUrl
            credentialsProvider = config.credentialsProvider()
        }
    }

    fun close() {
        client.close()
    }
}