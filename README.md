# AWS Lambda Stream for Kotlin (and Java)

A framework for building reliable, serverless, event-driven applications on AWS.

## Value Proposition

Building event-driven systems is fundamentally different from building traditional applications. Without a disciplined framework, teams can quickly end up with fragmented microservices, complex orchestration logic, and inconsistent handling of reliability concerns such as idempotency, eventual consistency, retries, and failure recovery.

This framework provides a consistent foundation for serverless event-driven architecture on AWS. It helps backend developers implement proven patterns faster, while giving enterprise architects confidence that solutions are reliable, maintainable, and aligned with architectural standards.

The project is currently a work in progress and is being used to explore which patterns and practices are most effective when building event-driven systems in a serverless environment.

This framework follows the serverless architecture patterns described by John Gilbert. I am using his typescript framework as a base of the implementation.
- [Book](https://a.co/d/0cgkIieB) 
- [Blog](https://medium.com/@jgilbert001)
- [Typescript framework](https://github.com/jgilbert01/aws-lambda-stream)

## Why Kotlin?

Kotlin was chosen because it is a modern language with a strong type system, concise syntax, and excellent support for building reusable libraries and components.

A key goal of this project is to make these components usable by Java developers as well. A Java-based example application will be added to demonstrate interoperability and usage from Java code.

## Project Structure

- `core` — Framework code and reusable components.
- `examples/sut` — Example application: Shipment Unit Tracking.
- `examples/sut/README.md` — Explains how to run the example application.



