package org.myorg.sut

import software.amazon.awscdk.Aws
import software.amazon.awscdk.RemovalPolicy
import software.amazon.awscdk.services.iam.*
import software.amazon.awscdk.services.s3.Bucket
import software.amazon.awscdk.services.s3.BucketAccessControl
import software.amazon.awscdk.services.s3.BucketEncryption
import software.amazon.awscdk.services.s3.CfnBucket

fun EventFaultMonitorStack.newBucket(): Bucket =
    Bucket.Builder.create(this, "Bucket")
        .bucketName(bucketName)
        .accessControl(BucketAccessControl.PRIVATE)
        .encryption(BucketEncryption.S3_MANAGED)
        .versioned(true)
        .removalPolicy(RemovalPolicy.RETAIN)
        .build()

fun EventFaultMonitorStack.replicationRegion(): String =
    when (regionName()) {
        "us-east-1" -> "us-west-2"
        "us-west-2" -> "us-east-1"
        else -> throw IllegalArgumentException("No replication region configured for ${regionName()}")
    }

fun EventFaultMonitorStack.replicationBucketArn(): String =
    "arn:${Aws.PARTITION}:s3:::${org()}-${service()}-${stage()}-${replicationRegion()}"

fun EventFaultMonitorStack.replicationRoleName(): String =
    "${service()}-${stage()}-${regionName()}-replicate"

fun EventFaultMonitorStack.newBucketReplicationRole(bucket: Bucket): Role {
    val role = Role.Builder.create(this, "BucketReplicationRole")
        .roleName(replicationRoleName())
        .assumedBy(ServicePrincipal("s3.amazonaws.com"))
        .inlinePolicies(
            mapOf(
                "replicate" to PolicyDocument.Builder.create()
                    .statements(
                        listOf(
                            PolicyStatement.Builder.create()
                                .effect(Effect.ALLOW)
                                .actions(
                                    listOf(
                                        "s3:GetReplicationConfiguration",
                                        "s3:ListBucket",
                                    )
                                )
                                .resources(
                                    listOf(
                                        bucket.bucketArn,
                                    )
                                )
                                .build(),
                            PolicyStatement.Builder.create()
                                .effect(Effect.ALLOW)
                                .actions(
                                    listOf(
                                        "s3:GetObjectVersion",
                                        "s3:GetObjectVersionAcl",
                                    )
                                )
                                .resources(
                                    listOf(
                                        "${bucket.bucketArn}/*",
                                    )
                                )
                                .build(),
                            PolicyStatement.Builder.create()
                                .effect(Effect.ALLOW)
                                .actions(
                                    listOf(
                                        "s3:ReplicateObject",
                                        "s3:ReplicateDelete",
                                        "s3:ObjectOwnerOverrideToBucketOwner",
                                    )
                                )
                                .resources(
                                    listOf(
                                        "${replicationBucketArn()}/*",
                                    )
                                )
                                .build(),
                        )
                    )
                    .build()
            )
        )
        .build()

    role.node.addDependency(bucket)
    return role
}

fun EventFaultMonitorStack.newBucketReplicationPolicy(bucket: Bucket) {
    bucket.addToResourcePolicy(
        PolicyStatement.Builder.create()
            .effect(Effect.ALLOW)
            .principals(
                listOf(
                    AccountPrincipal(Aws.ACCOUNT_ID),
                )
            )
            .actions(
                listOf(
                    "s3:ReplicateDelete",
                    "s3:ReplicateObject",
                    "s3:ObjectOwnerOverrideToBucketOwner",
                )
            )
            .resources(
                listOf(
                    "${bucket.bucketArn}/*",
                )
            )
            .build()
    )
}

fun EventFaultMonitorStack.configureBucketReplication(
    bucket: Bucket,
    bucketReplicationRole: Role,
) {
    val cfnBucket = bucket.node.defaultChild as CfnBucket

    val replicationConfig =
        CfnBucket.ReplicationConfigurationProperty.builder()
            .role(bucketReplicationRole.roleArn)
            .rules(
                listOf(
                    CfnBucket.ReplicationRuleProperty.builder()
                        .id("Replication")
                        .status("Enabled")
                        .prefix("")
                        .destination(
                            CfnBucket.ReplicationDestinationProperty.builder()
                                .bucket(replicationBucketArn())
                                .build()
                        )
                        .build()
                )
            )
            .build()

    cfnBucket.setReplicationConfiguration(replicationConfig)
    cfnBucket.node.addDependency(bucketReplicationRole)
}



