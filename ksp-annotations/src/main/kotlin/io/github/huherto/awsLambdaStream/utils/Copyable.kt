package io.github.huherto.awsLambdaStream.utils

/**
 * Marks a class for KSP-based field copying.
 * The KSP processor will generate a companion extension or helper
 * to perform reflection-free copying.
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.SOURCE)
annotation class Copyable
