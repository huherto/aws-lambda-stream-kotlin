package org.myorg.payments.spring

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.kopipes.aws.AwsKopipesContext
import io.kopipes.aws.sinks.DynamoDbEventsMicrostore
import io.kopipes.core.sinks.EventsMicrostore
import io.kopipes.spring.lambda.SpringDynamoDbHandler
import io.kopipes.spring.lambda.SpringKinesisHandler
import io.kopipes.spring.lambda.dynamoDbConsumer
import io.kopipes.spring.lambda.kinesisConsumer
import org.myorg.payments.adapters.inbound.ListenerContainer
import org.myorg.payments.adapters.inbound.TriggerContainer
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.function.Consumer

/**
 * The payment control service on Spring Boot. The domain, the adapters and the containers are the plain service's
 * (`:app`); Spring only replaces the composition root. kopipes' auto-configuration supplies the [AwsKopipesContext]
 * bean, read from the same env vars.
 */
@Configuration(proxyBeanMethods = false)
class ControlServiceConfiguration {

    @Bean
    fun eventsMicrostore(kopipes: AwsKopipesContext): EventsMicrostore = DynamoDbEventsMicrostore(kopipes)

    @Bean
    fun listenerContainer(kopipes: AwsKopipesContext, eventsMicrostore: EventsMicrostore): ListenerContainer =
        ListenerContainer(kopipes, eventsMicrostore)

    @Bean
    fun triggerContainer(
        kopipes: AwsKopipesContext,
        eventsMicrostore: EventsMicrostore,
        @Value("\${DEADLINE_SECONDS:${TriggerContainer.DEFAULT_DEADLINE_SECONDS}}") deadlineSeconds: Long,
    ): TriggerContainer = TriggerContainer(kopipes, eventsMicrostore, deadlineSeconds = deadlineSeconds)

    /** Default channel → collect payment events into the events table. */
    @Bean
    fun listener(container: ListenerContainer): Consumer<KinesisEvent> =
        kinesisConsumer(container.assembler, container.kinesisAdapter)

    /** Events table stream → correlate, evaluate the rules, publish. */
    @Bean
    fun trigger(container: TriggerContainer): Consumer<DynamodbEvent> =
        dynamoDbConsumer(container.assembler, container.dynamoDbAdapter)
}

@SpringBootApplication(proxyBeanMethods = false)
class ControlServiceApplication

/** Lambda handler: `org.myorg.payments.spring.Listener::handleRequest`. */
class Listener : SpringKinesisHandler(ControlServiceApplication::class.java)

/** Lambda handler: `org.myorg.payments.spring.Trigger::handleRequest`. */
class Trigger : SpringDynamoDbHandler(ControlServiceApplication::class.java)
