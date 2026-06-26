package org.myorg.sut

import software.amazon.awscdk.services.iam.Effect
import software.amazon.awscdk.services.iam.PolicyStatement
import software.amazon.awscdk.services.lambda.Function

fun RegionalHealthCheckStack.masterKeyArn(): String =
    environment().getValue("MASTER_KEY_ARN")

fun RegionalHealthCheckStack.addKmsPermissions(function: Function) {
    function.addToRolePolicy(
        PolicyStatement.Builder.create()
            .effect(Effect.ALLOW)
            .actions(
                listOf(
                    "kms:ListKeys",
                    "kms:ListAliases",
                    "kms:GenerateDataKey",
                    "kms:Encrypt",
                    "kms:Decrypt",
                )
            )
            .resources(
                listOf(
                    masterKeyArn(),
                )
            )
            .build()
    )
}