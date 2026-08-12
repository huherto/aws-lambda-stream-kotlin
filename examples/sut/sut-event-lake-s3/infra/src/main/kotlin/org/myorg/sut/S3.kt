package org.myorg.sut

import software.amazon.awscdk.Aws
import software.amazon.awscdk.CfnOutput
import software.amazon.awscdk.Duration
import software.amazon.awscdk.RemovalPolicy
import software.amazon.awscdk.services.iam.*
import software.amazon.awscdk.services.s3.*

fun EventLakeS3Stack.newBucket(): Bucket =
    Bucket.Builder.create(this, "Bucket")
        .bucketName(bucketName())
        .accessControl(BucketAccessControl.PRIVATE)
        .encryption(BucketEncryption.S3_MANAGED)
        .versioned(true)
        .removalPolicy(RemovalPolicy.RETAIN)
        .build()

fun EventLakeS3Stack.bucketName(): String =
    "${org()}-${service()}-${stage()}-${regionName()}"

fun EventLakeS3Stack.replicationRegion(): String =
    when (regionName()) {
        "us-east-1" -> "us-west-2"
        "us-west-2" -> "us-east-1"
        else -> throw IllegalArgumentException("No replication region configured for ${regionName()}")
    }

fun EventLakeS3Stack.replicationBucketArn(): String =
    "arn:${Aws.PARTITION}:s3:::${org()}-${service()}-${stage()}-${replicationRegion()}"

fun EventLakeS3Stack.replicationRoleName(): String =
    "${service()}-${stage()}-${regionName()}-replicate"

fun EventLakeS3Stack.outputBucketDetails(bucket: Bucket) {
    CfnOutput.Builder.create(this, "BucketName")
        .value(bucket.bucketName)
        .build()

    CfnOutput.Builder.create(this, "BucketArn")
        .value(bucket.bucketArn)
        .build()
}

/**
 * Translates the commented LifecycleConfiguration block:
 *
 * LifecycleConfiguration:
 *   Rules:
 *     - Prefix: ''
 *       Status: Enabled
 *       ExpirationInDays: 92
 *
 * Call this instead of newBucket() if you want lifecycle expiration enabled.
 */
fun EventLakeS3Stack.newBucketWithLifecycle(): Bucket =
    Bucket.Builder.create(this, "Bucket")
        .bucketName(bucketName())
        .accessControl(BucketAccessControl.PRIVATE)
        .encryption(BucketEncryption.S3_MANAGED)
        .versioned(true)
        .lifecycleRules(
            listOf(
                LifecycleRule.builder()
                    .prefix("")
                    .enabled(true)
                    .expiration(Duration.days(92))
                    .build()
            )
        )
        .removalPolicy(RemovalPolicy.RETAIN)
        .build()

/**
 * Translates the commented LoggingConfiguration block:
 *
 * LoggingConfiguration:
 *   DestinationBucketName: ${self:custom.org}-${self:custom.subsys}-logs-${opt:region}
 *
 * Call this after creating the bucket if you want server access logging enabled.
 */
fun EventLakeS3Stack.configureBucketLogging(bucket: Bucket) {
    val cfnBucket = bucket.node.defaultChild as CfnBucket

    cfnBucket.setLoggingConfiguration(
        CfnBucket.LoggingConfigurationProperty.builder()
            .destinationBucketName("${org()}-${subsys()}-logs-${regionName()}")
            .build()
    )
}

/**
 * Translates the commented BucketReplicationRole resource.
 *
 * Call this if replication should be enabled.
 */
fun EventLakeS3Stack.newBucketReplicationRole(bucket: Bucket): Role {
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
                                        bucket.arnForObjects("*"),
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

/**
 * Translates the commented BucketPolicy resource.
 *
 * Call this on the destination bucket stack if this bucket should allow replication writes.
 */
fun EventLakeS3Stack.grantReplicationAccess(bucket: Bucket) {
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
                    bucket.arnForObjects("*"),
                )
            )
            .build()
    )
}

/**
 * Translates the commented ReplicationConfiguration block.
 *
 * Call this only after the target bucket has already been deployed in the mirrored region.
 */
fun EventLakeS3Stack.replicateToMirrorRegion(
    bucket: Bucket,
    bucketReplicationRole: Role,
) {
    val cfnBucket = bucket.node.defaultChild as CfnBucket

    cfnBucket.setReplicationConfiguration(
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
    )

    cfnBucket.node.addDependency(bucketReplicationRole)
}