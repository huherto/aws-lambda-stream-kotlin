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
choice for this framework. In particular, support for coroutines and the Flow framework make it a natural fit for building
[serverless pipelines](docs/KotlinCoRoutinesAndFlow.md)


A key goal of this project is to make these components usable by Java developers as well. A Java-based example application 
will be added to demonstrate interoperability and usage from Java code.

## Project Structure

- [core](core) — Framework code and reusable components.
- [examples/sut](examples/sut) — Example application: Shipment Unit Tracking.
- [examples/sut/README.md](examples/sut/README.md) — Explains how to run the example application.
- [docs](docs) — Documentation for the framework.
