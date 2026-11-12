package io.kopipes.spring

import io.kopipes.aws.AwsEnvironmentConfig
import io.kopipes.aws.AwsKopipesContext
import io.kopipes.core.serialization.snapshots.DefaultUnitOfWorkSnapshotter
import io.kopipes.core.serialization.snapshots.RecordSnapshotter
import io.kopipes.aws.captureAwsOperations
import org.springframework.beans.factory.ObjectProvider
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment

/**
 * kopipes for Spring Framework without Spring Boot: `@Import(KopipesConfiguration::class)`.
 *
 * Uses only `spring-context`. It defines the [AwsEnvironmentConfig], read from the Spring [Environment] like
 * [SpringEnvironmentConfig] does, and the [AwsKopipesContext] built from it with its defaults (EventBridge publisher,
 * AWS-aware fault manager, lazy clients); any [RecordSnapshotter] beans are added to its snapshotter. Inject the
 * context and build components from it.
 *
 * To change a collaborator (an in-memory publisher in a test, a custom retry rule), don't import this: declare your own
 * `AwsKopipesContext` bean instead. Spring Boot apps don't need it at all: the auto-configuration covers them, with every
 * piece replaceable.
 */
@Configuration(proxyBeanMethods = false)
class KopipesConfiguration {

    @Bean
    fun kopipesEnvironmentConfig(environment: Environment): AwsEnvironmentConfig = SpringEnvironmentConfig(environment)

    @Bean
    fun kopipesContext(
        envConfig: AwsEnvironmentConfig,
        recordSnapshotters: ObjectProvider<RecordSnapshotter>,
    ): AwsKopipesContext = AwsKopipesContext(
        envConfig = envConfig,
        unitOfWorkSnapshotter = DefaultUnitOfWorkSnapshotter(
            recordSnapshotters = recordSnapshotters.orderedStream().toList() + AwsKopipesContext.defaultRecordSnapshotters(),
            awsOperationCapturer = ::captureAwsOperations,
        ),
    )
}
