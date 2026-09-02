# Software Architects Guide

This framework provides a consistent foundation for serverless event-driven architecture on AWS. It helps enterprise architects ensure that solutions are reliable, maintainable, and aligned with architectural standards.

## Value Proposition

Building event-driven systems is fundamentally different from building traditional applications. Without a disciplined framework, teams can end up with fragmented services, complex orchestration logic, and inconsistent handling of reliability concerns such as idempotency, retries, eventual consistency, and failure recovery.

AWS Lambda Stream for the JVM implements proven patterns to address these concerns:

- **Consistency**: Unified foundation for all serverless event-driven services.
- **Reliability**: Built-in handling for idempotency, eventual consistency, and failure recovery.
- **Standardization**: Aligns solutions with enterprise architectural standards.

## Architectural Patterns

The framework follows the serverless architecture patterns described by John Gilbert in his [book](https://a.co/d/0cgkIieB).

* [Architectural Approach](ArchitecturalApproach.md)
* [Autonomous Service Patterns](AutonomousServicePatterns.md)
* [How Claim Check works](ClaimCheck.md)
* [How the Events Microstore works](EventsMicrostore.md)
* [How the Event Lake works](EventLake.md)
* [How the Regional Health Check works](RegionalHealthCheck.md)
* [How Fault Re-submission works](FaultResubmission.md)
