package io.kopipes.core.utils

import mu.KLogger

/**
 * Wraps [lazy] initialization with lifecycle logging and direct [System.err] output on failure,
 * ensuring initialization diagnostics and stack traces are reliably flushed in AWS Lambda cold starts.
 */
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