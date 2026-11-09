package org.myorg.payments

import software.amazon.awscdk.Aws
import software.amazon.awscdk.Duration
import software.amazon.awscdk.Stack
import software.amazon.awscdk.services.kinesis.IStream
import software.amazon.awscdk.services.kinesis.Stream
import software.amazon.awscdk.services.lambda.Function
import software.amazon.awscdk.services.lambda.StartingPosition
import software.amazon.awscdk.services.lambda.eventsources.KinesisEventSource
import software.constructs.Construct

enum class Stage(val id: String) {
    LOCAL("local"),
    DEV("dev"),
    QA("qa"),
    PROD("prod");

    override fun toString(): String = id

    companion object {
        fun parse(value: String): Stage =
            entries.firstOrNull { it.id.equals(value, ignoreCase = true) }
                ?: throw IllegalArgumentException("Unknown stage: $value")
    }
}

data class ServiceProps(val org: String, val subsys: String, val service: String, val stage: Stage, val region: String)

/** The channels of the payments event hub, by the name each consumer imports. */
object Channels {
    /** Internal events (source != external), all but faults. */
    const val DEFAULT = "s1"

    /** External ACCOUNTS.* events, read only by the accounts ingress gateway. */
    const val INGRESS = "ingress"
}

open class BaseStack(scope: Construct, val serviceProps: ServiceProps) :
    Stack(scope, "${serviceProps.service}-${serviceProps.stage}") {

    fun org() = serviceProps.org
    fun subsys() = serviceProps.subsys
    fun service() = serviceProps.service
    fun stage() = serviceProps.stage
    fun regionName() = serviceProps.region

    /** The subsystem's bus, owned by the event hub stack. */
    fun busName() = "${subsys()}-event-hub-${stage()}-bus"

    /** A Kinesis channel of the event hub, by its short name (see [Channels]). */
    fun channelName(channel: String) = "${subsys()}-event-hub-${stage()}-$channel"

    /** Environment shared by every Lambda function of the subsystem. */
    fun baseLambdaEnvironment(): Map<String, String> = mapOf(
        // Tiered compilation stopped at C1 and the serial GC: faster, lighter cold starts for short-lived functions.
        "JAVA_TOOL_OPTIONS" to "-Dslf4j.provider=io.github.vitalijr2.aws.lambda.slf4j.AWSLambdaServiceProvider " +
            "-XX:+TieredCompilation -XX:TieredStopAtLevel=1 -XX:+UseSerialGC",
        "LOG_DEFAULT_LEVEL" to "DEBUG",
        "STAGE" to stage().id,
        // tags.source on every event this service publishes.
        "SERVICE" to service(),
        "BUS_NAME" to busName(),
        // The bus's own routing attribute: the producing service for internal events.
        "BUS_SRC" to service(),
        "SERIALIZATION_STRATEGY" to "KOTLINX",
    )

    /** Imports one of the event hub's Kinesis channels. */
    fun importChannel(id: String, channel: String): IStream {
        val streamArn = "arn:aws:kinesis:${Aws.REGION}:${Aws.ACCOUNT_ID}:stream/${channelName(channel)}"
        return Stream.fromStreamArn(this, id, streamArn)
    }

    /**
     * Wires a Lambda function to read a Kinesis channel at its own pace. A failed batch is retried
     * [retryAttempts] times (`-1`: until it succeeds or its records expire), so this consumer waits while the others
     * carry on: each consumer has its own position on the channel.
     */
    fun consumeFromKinesis(listener: Function, stream: IStream, retryAttempts: Int = 3) {
        listener.addEventSource(
            KinesisEventSource.Builder.create(stream)
                .startingPosition(StartingPosition.TRIM_HORIZON)
                .batchSize(100)
                .maxBatchingWindow(Duration.seconds(1))
                .bisectBatchOnError(true)
                .retryAttempts(retryAttempts)
                .parallelizationFactor(1)
                .build()
        )
    }
}
