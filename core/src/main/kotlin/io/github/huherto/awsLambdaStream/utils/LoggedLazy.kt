package io.github.huherto.awsLambdaStream.utils

import mu.KLogger

fun <T> loggedLazy(
    name: String,
    logger: KLogger,
    initializer: () -> T,
): Lazy<T> = lazy {
    logger.info { "Initializing $name" }

    try {
        initializer().also {
            logger.info { "$name initialized" }
        }
    } catch (error: Throwable) {
        System.err.println("Failed to initialize $name: ${error.message}")
        error.printStackTrace(System.err)

        logger.error(error) { "Failed to initialize $name" }
        throw error
    }
}