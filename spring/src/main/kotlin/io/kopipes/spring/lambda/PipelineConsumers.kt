package io.kopipes.spring.lambda

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.kopipes.aws.from.DynamodbAdapter
import io.kopipes.aws.from.KinesisAdapter
import io.kopipes.aws.from.SqsAdapter
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.PipelineRunner
import io.kopipes.core.UnitOfWork
import kotlinx.coroutines.flow.Flow
import java.util.function.Consumer

/**
 * A [Consumer] that runs every event it accepts through [assembler], with [headFlow] turning the event into units of
 * work. Declare it as a `@Bean` of type `Consumer<EventType>`: the `Spring*Handler` classes find it by that type, and it is
 * also the shape Spring Cloud Function expects.
 */
fun <E> pipelineConsumer(assembler: PipelineAssembler, headFlow: (E) -> Flow<UnitOfWork>): Consumer<E> {
    val runner = PipelineRunner<E>(assembler).headFlow(headFlow)
    return Consumer { event -> runner.run(event) }
}

/** Kinesis records → [assembler]. */
fun kinesisConsumer(assembler: PipelineAssembler, adapter: KinesisAdapter): Consumer<KinesisEvent> =
    pipelineConsumer(assembler) { event -> adapter.fromKinesis(event) }

/** DynamoDB stream records → [assembler]. */
fun dynamoDbConsumer(assembler: PipelineAssembler, adapter: DynamodbAdapter): Consumer<DynamodbEvent> =
    pipelineConsumer(assembler) { event -> adapter.fromDynamoDB(event) }

/** SQS messages (each carrying an event) → [assembler]. */
fun sqsConsumer(assembler: PipelineAssembler, adapter: SqsAdapter): Consumer<SQSEvent> =
    pipelineConsumer(assembler) { event -> adapter.fromSqsEvent(event) }
