# AWS Lambda Stream for Kotlin and Java

A Kotlin-first framework for building reliable, serverless, event-driven applications on AWS, with Java interoperability in mind.

## Value Proposition

Building event-driven systems is fundamentally different from building traditional applications. Without a disciplined framework, teams can end up with fragmented services, complex orchestration logic, and inconsistent handling of reliability concerns such as idempotency, retries, eventual consistency, and failure recovery.

This framework provides a consistent foundation for serverless event-driven architecture on AWS. It helps backend developers implement proven patterns faster, while giving enterprise architects confidence that solutions are reliable, maintainable, and aligned with architectural standards.

The project is currently a work in progress. Feedback, ideas, and experimental usage are welcome as the framework evolves.

This framework follows the serverless architecture patterns described by John Gilbert in his [book](https://a.co/d/0cgkIieB). This implementation uses his TypeScript framework as a reference and foundation.
- [Blog](https://medium.com/@jgilbert001)
- [TypeScript framework](https://github.com/jgilbert01/aws-lambda-stream)

## Why Kotlin?

Kotlin was chosen because it is a modern language with a strong type system, concise syntax, and excellent support for
building reusable libraries and components. It is also a great fit for serverless applications, making it an ideal
choice for this framework.

### Coroutines and Flow for Serverless Pipelines

Kotlin's coroutines and Flow framework provide significant advantages when building serverless event-driven pipelines:

**Asynchronous Processing Without Blocking**: Coroutines enable efficient asynchronous I/O operations (like S3
reads/writes, DynamoDB queries, or SQS polling) without blocking threads. This is critical in Lambda environments where
compute resources are billed by execution time and memory usage.

**Backpressure Handling**: Flow provides built-in backpressure mechanisms through operators like `buffer()`. When
processing event streams (like the claim-check pattern in `ClaimCheckStore`), Flow automatically manages the rate at
which events are processed, preventing memory exhaustion when downstream consumers are slower than upstream producers.

**Composable Pipeline Stages**: Flow operators (`map`, `filter`, `mapNotNull`, etc.) allow building complex event
processing pipelines from small, testable, reusable components. For example, the `SqsAdapter.fromSqsEvent()`
demonstrates chaining decode → filter → redeem operations in a clear, declarative style.

**Structured Concurrency**: Coroutines enforce structured concurrency, ensuring all child operations complete or cancel
together. This prevents resource leaks in Lambda functions and simplifies error handling across pipeline stages.

**Efficient Batching**: Operators like `buffer(capacity)` enable controlled concurrent processing of batched events. The
`ClaimCheckStore.storeClaimCheck()` uses this to process multiple S3 uploads concurrently while maintaining ordering and
fault tolerance guarantees.

**Cold Flow Semantics**: Flows are cold streams—they don't execute until collected. This lazy evaluation is ideal for
Lambda's request-driven model, avoiding unnecessary work and enabling efficient resource usage.

A key goal of this project is to make these components usable by Java developers as well. A Java-based example application 
will be added to demonstrate interoperability and usage from Java code.

## Project Structure

- `core` — Framework code and reusable components.
- `examples/sut` — Example application: Shipment Unit Tracking.
- `examples/sut/README.md` — Explains how to run the example application.
- `docs` — Documentation for the framework.
