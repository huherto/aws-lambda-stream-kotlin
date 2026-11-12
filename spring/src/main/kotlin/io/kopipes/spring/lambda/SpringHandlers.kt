package io.kopipes.spring.lambda

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import mu.KotlinLogging
import org.springframework.boot.Banner
import org.springframework.boot.WebApplicationType
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.core.ResolvableType
import org.springframework.core.annotation.MergedAnnotations
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Consumer

/**
 * One Spring application context per application class per JVM: a Lambda container starts it on its first (init) call
 * and reuses it for every invocation after that.
 *
 * A `@SpringBootApplication` (any `@SpringBootConfiguration`) starts as a Spring Boot application. Any other
 * `@Configuration` starts in a plain `AnnotationConfigApplicationContext`, so Spring Framework apps without Boot need
 * only `spring-context` on the classpath.
 */
object SpringLambdaContexts {
    private val logger = KotlinLogging.logger { }
    private val contexts = ConcurrentHashMap<Class<*>, ConfigurableApplicationContext>()

    fun get(application: Class<*>): ConfigurableApplicationContext =
        contexts.computeIfAbsent(application) {
            val started = System.currentTimeMillis()
            val boot = isSpringBootApplication(application)
            val context = if (boot) BootContexts.start(application) else AnnotationConfigApplicationContext(application)
            logger.info {
                "Spring ${if (boot) "Boot " else ""}context for ${application.simpleName} started in " +
                    "${System.currentTimeMillis() - started} ms"
            }
            context
        }

    // By name: reading the annotation must not load Spring Boot classes when Boot isn't on the classpath.
    internal fun isSpringBootApplication(application: Class<*>): Boolean =
        MergedAnnotations.from(application, MergedAnnotations.SearchStrategy.TYPE_HIERARCHY)
            .isPresent("org.springframework.boot.SpringBootConfiguration")

    /** The `Consumer<eventType>` bean of [application]: the one named [beanName], or the only one of that type. */
    fun <E> consumer(application: Class<*>, eventType: Class<E>, beanName: String? = null): Consumer<E> {
        val context = get(application)
        @Suppress("UNCHECKED_CAST")
        return if (beanName != null) {
            context.getBean(beanName, Consumer::class.java) as Consumer<E>
        } else {
            val type = ResolvableType.forClassWithGenerics(Consumer::class.java, eventType)
            context.getBeanProvider<Consumer<E>>(type).getObject()
        }
    }

    /** Closes every context; for tests. */
    fun closeAll() {
        contexts.values.forEach { it.close() }
        contexts.clear()
    }
}

/** The only code that touches Spring Boot classes; loaded only for Spring Boot applications. */
private object BootContexts {
    fun start(application: Class<*>): ConfigurableApplicationContext =
        SpringApplicationBuilder(application)
            .web(WebApplicationType.NONE)
            .bannerMode(Banner.Mode.OFF)
            .run()
}

/*
 * The handlers below implement RequestHandler with concrete event types, so the Lambda runtime can read the input type
 * from the class. Subclass one per function, naming your @SpringBootApplication, and point the function's handler at
 * the subclass:
 *
 *   class Listener : SpringKinesisHandler(ControlServiceApplication::class.java)
 *
 * The context starts in the constructor, during the Lambda init phase (where SnapStart can snapshot it).
 */

/** Kinesis → the application's `Consumer<KinesisEvent>` bean. */
abstract class SpringKinesisHandler @JvmOverloads constructor(application: Class<*>, beanName: String? = null) :
    RequestHandler<KinesisEvent, Void?> {
    private val consumer = SpringLambdaContexts.consumer(application, KinesisEvent::class.java, beanName)

    override fun handleRequest(event: KinesisEvent, context: Context): Void? {
        consumer.accept(event)
        return null
    }
}

/** DynamoDB stream → the application's `Consumer<DynamodbEvent>` bean. */
abstract class SpringDynamoDbHandler @JvmOverloads constructor(application: Class<*>, beanName: String? = null) :
    RequestHandler<DynamodbEvent, Void?> {
    private val consumer = SpringLambdaContexts.consumer(application, DynamodbEvent::class.java, beanName)

    override fun handleRequest(event: DynamodbEvent, context: Context): Void? {
        consumer.accept(event)
        return null
    }
}

/** SQS → the application's `Consumer<SQSEvent>` bean. */
abstract class SpringSqsHandler @JvmOverloads constructor(application: Class<*>, beanName: String? = null) :
    RequestHandler<SQSEvent, Void?> {
    private val consumer = SpringLambdaContexts.consumer(application, SQSEvent::class.java, beanName)

    override fun handleRequest(event: SQSEvent, context: Context): Void? {
        consumer.accept(event)
        return null
    }
}
