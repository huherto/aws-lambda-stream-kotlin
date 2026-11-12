package io.kopipes.spring.lambda.app

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.kopipes.aws.AwsKopipesContext
import io.kopipes.aws.from.DynamodbAdapter
import io.kopipes.aws.from.KinesisAdapter
import io.kopipes.core.JsonEventCodec
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.flavors.CollectPipeline
import io.kopipes.core.sinks.EventPublisher
import io.kopipes.core.sinks.EventPublisherInMemory
import io.kopipes.core.sinks.EventsMicrostoreInMemory
import io.kopipes.spring.lambda.SpringDynamoDbHandler
import io.kopipes.spring.lambda.SpringKinesisHandler
import io.kopipes.spring.lambda.dynamoDbConsumer
import io.kopipes.spring.lambda.kinesisConsumer

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import java.util.function.Consumer

/** A small stream application: Kinesis events are collected into an in-memory microstore. */
@SpringBootApplication(proxyBeanMethods = false)
class TestStreamApplication {

    @Bean
    fun eventPublisher(): EventPublisher = EventPublisherInMemory()

    @Bean
    fun microstore(): EventsMicrostoreInMemory = EventsMicrostoreInMemory()

    @Bean
    fun assembler(kopipes: AwsKopipesContext, microstore: EventsMicrostoreInMemory): PipelineAssembler =
        PipelineAssembler.builder()
            .context(kopipes)
            .addPipeline(CollectPipeline.builder().id("collect").eventsMicrostore(microstore).build())
            .build()

    @Bean
    fun listener(assembler: PipelineAssembler, kopipes: AwsKopipesContext): Consumer<KinesisEvent> =
        kinesisConsumer(assembler, KinesisAdapter(kopipes, JsonEventCodec))

    @Bean
    fun trigger(assembler: PipelineAssembler, kopipes: AwsKopipesContext): Consumer<DynamodbEvent> =
        dynamoDbConsumer(assembler, DynamodbAdapter(kopipes))

    /** A second Kinesis consumer, so handlers must pick one by name. Counts what it sees. */
    @Bean
    fun audit(assembler: PipelineAssembler, kopipes: AwsKopipesContext, seen: AuditLog): Consumer<KinesisEvent> =
        Consumer { event -> seen.records += event.records.size }

    @Bean
    fun auditLog(): AuditLog = AuditLog()
}

class AuditLog {
    var records = 0
}

class TestListener : SpringKinesisHandler(TestStreamApplication::class.java, "listener")

class TestAudit : SpringKinesisHandler(TestStreamApplication::class.java, "audit")

class TestTrigger : SpringDynamoDbHandler(TestStreamApplication::class.java)

