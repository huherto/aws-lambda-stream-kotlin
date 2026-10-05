package io.kopipes.aws

import aws.smithy.kotlin.runtime.SdkBaseException
import io.kopipes.aws.connectors.*
import io.kopipes.aws.extensions.*
import io.kopipes.aws.serialization.snapshots.DynamoDbRecordSnapshotter
import io.kopipes.aws.serialization.snapshots.KinesisRecordSnapshotter
import io.kopipes.aws.serialization.snapshots.SqsRecordSnapshotter
import io.kopipes.aws.sinks.EventBridgePublisher
import io.kopipes.core.*
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.serialization.snapshots.AwsOperationSnapshot
import io.kopipes.core.serialization.snapshots.DefaultUnitOfWorkSnapshotter

/** Registry for AWS specific client factories and global configuration. */
object AwsGlobalRegistry {

    private val lock = Any()

    val isAwsRetryable: (Throwable) -> Boolean = { throwable ->
        if (throwable is SdkBaseException) {
            throwable.sdkErrorMetadata.isRetryable
        } else {
            false
        }
    }

    fun captureAwsOperations(uow: UnitOfWork): List<AwsOperationSnapshot>? {
        val ops = mutableListOf<AwsOperationSnapshot>()

        uow.publishResponse?.let {
            ops.add(AwsOperationSnapshot(service = "EventBridge", operation = "PutEvents"))
        }
        uow.putMetricDataResponse?.let {
            ops.add(AwsOperationSnapshot(service = "CloudWatch", operation = "PutMetricData"))
        }
        uow.putResponse?.let {
            ops.add(AwsOperationSnapshot(service = "DynamoDB", operation = "PutItem"))
        }
        uow.updateResponse?.let {
            ops.add(AwsOperationSnapshot(service = "DynamoDB", operation = "UpdateItem"))
        }
        uow.queryResponse?.let {
            ops.add(AwsOperationSnapshot(service = "DynamoDB", operation = "Query"))
        }
        uow.scanRequest?.let {
            ops.add(AwsOperationSnapshot(service = "DynamoDB", operation = "Scan"))
        }
        uow.batchGetResponse?.let {
            ops.add(AwsOperationSnapshot(service = "DynamoDB", operation = "BatchGetItem"))
        }

        return if (ops.isEmpty()) null else ops
    }

    private val dynamoDbClientFactorySingleton = RegistrySingleton(
        lock = lock,
        defaultFactory = { DefaultDynamoDbClientFactory() as DynamoDbClientFactory }
    )

    private val eventBridgeClientFactorySingleton = RegistrySingleton(
        lock = lock,
        defaultFactory = { DefaultEventBridgeClientFactory() as EventBridgeClientFactory }
    )

    private val s3ClientFactorySingleton = RegistrySingleton(
        lock = lock,
        defaultFactory = { DefaultS3ClientFactory() as S3ClientFactory }
    )

    private val kmsClientFactorySingleton = RegistrySingleton(
        lock = lock,
        defaultFactory = { DefaultKmsClientFactory() as KmsClientFactory }
    )

    private val cloudWatchClientFactorySingleton = RegistrySingleton(
        lock = lock,
        defaultFactory = { DefaultCloudWatchClientFactory() as CloudWatchClientFactory }
    )

    @JvmStatic
    fun dynamoDbClientFactory(): DynamoDbClientFactory {
        return dynamoDbClientFactorySingleton.get()
    }

    @JvmStatic
    fun setDynamoDbClientFactory(factory: DynamoDbClientFactory) {
        dynamoDbClientFactorySingleton.set(factory)
    }

    @JvmStatic
    fun setDynamoDbClientFactory(factory: () -> DynamoDbClientFactory) {
        dynamoDbClientFactorySingleton.setFactory(factory)
    }

    @JvmStatic
    fun eventBridgeClientFactory(): EventBridgeClientFactory {
        return eventBridgeClientFactorySingleton.get()
    }

    @JvmStatic
    fun setEventBridgeClientFactory(factory: EventBridgeClientFactory) {
        eventBridgeClientFactorySingleton.set(factory)
    }

    @JvmStatic
    fun setEventBridgeClientFactory(factory: () -> EventBridgeClientFactory) {
        eventBridgeClientFactorySingleton.setFactory(factory)
    }

    @JvmStatic
    fun s3ClientFactory(): S3ClientFactory {
        return s3ClientFactorySingleton.get()
    }

    @JvmStatic
    fun setS3ClientFactory(factory: S3ClientFactory) {
        s3ClientFactorySingleton.set(factory)
    }

    @JvmStatic
    fun setS3ClientFactory(factory: () -> S3ClientFactory) {
        s3ClientFactorySingleton.setFactory(factory)
    }

    @JvmStatic
    fun kmsClientFactory(): KmsClientFactory {
        return kmsClientFactorySingleton.get()
    }

    @JvmStatic
    fun setKmsClientFactory(factory: KmsClientFactory) {
        kmsClientFactorySingleton.set(factory)
    }

    @JvmStatic
    fun setKmsClientFactory(factory: () -> KmsClientFactory) {
        kmsClientFactorySingleton.setFactory(factory)
    }

    @JvmStatic
    fun cloudWatchClientFactory(): CloudWatchClientFactory {
        return cloudWatchClientFactorySingleton.get()
    }

    @JvmStatic
    fun setCloudWatchClientFactory(factory: CloudWatchClientFactory) {
        cloudWatchClientFactorySingleton.set(factory)
    }

    @JvmStatic
    fun setCloudWatchClientFactory(factory: () -> CloudWatchClientFactory) {
        cloudWatchClientFactorySingleton.setFactory(factory)
    }

    @JvmStatic
    fun init() {
        GlobalRegistry.setEnvConfigFactory { AwsEnvironmentConfig() }
        GlobalRegistry.setEventPublisherFactory { EventBridgePublisher() }
        GlobalRegistry.setFaultManagerFactory {
            FaultManager(
                eventPublisher = GlobalRegistry.eventPublisher(),
                isRetryable = isAwsRetryable
            )
        }
        GlobalRegistry.registerRecordSnapshotter(KinesisRecordSnapshotter())
        GlobalRegistry.registerRecordSnapshotter(DynamoDbRecordSnapshotter())
        GlobalRegistry.registerRecordSnapshotter(SqsRecordSnapshotter())
        RawRecordSerializer.register(RAW_DYNAMODB, DynamodbRaw.serializer())
        RawRecordSerializer.register(RAW_IMAGES, ImagesRaw.serializer())
        RawRecordSerializer.register(RAW_KINESIS, KinesisRaw.serializer())
        RawRecordSerializer.register(RAW_SQS, SqsRaw.serializer())
        RawRecordSerializer.register(RAW_CLAIM_CHECK, ClaimCheck.serializer())
        GlobalRegistry.setUnitOfWorkSnapshotterFactory {
            DefaultUnitOfWorkSnapshotter(
                recordSnapshotters = GlobalRegistry.registeredRecordSnapshotters(),
                awsOperationCapturer = ::captureAwsOperations
            )
        }
    }

    init {
        init()
    }

    @JvmStatic
    fun reset() {
        synchronized(lock) {
            dynamoDbClientFactorySingleton.reset()
            eventBridgeClientFactorySingleton.reset()
            s3ClientFactorySingleton.reset()
            kmsClientFactorySingleton.reset()
            cloudWatchClientFactorySingleton.reset()
            GlobalRegistry.reset()
            init()
        }
    }
}
