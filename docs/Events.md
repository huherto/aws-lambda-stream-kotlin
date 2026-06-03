# Events
Events are the primary mechanism for communication between services and subsystems. They are used to trigger actions, coordinate business processes, and facilitate communication between services and subsystems.

## Table of Contents

- [Domain Events](#domain-events)
    - [Internal Domain Events](#internal-domain-events)
    - [External Domain Events](#external-domain-events)
- [Change Events](#change-events)

## Domain Events

Domain Events represent significant business occurrences within the system. They capture facts about what has happened
in the domain and are named in the past tense to reflect completed actions (e.g., "OrderPlaced", "PaymentProcessed", "
CustomerRegistered").

Domain Events are:

- Immutable records of business facts
- Published when significant state changes occur
- Used to communicate business-level changes between services and subsystems
- The foundation for event-driven architecture and event sourcing patterns

### Internal Domain Events

Internal Domain Events represent significant business occurrences that are relevant within a single subsystem. They are
used for communication between services within the same subsystem boundary and are not exposed to external subsystems.

Internal Domain Events are:

- Scoped to a single subsystem
- Used to coordinate business processes within subsystem boundaries
- May have subsystem-specific payload structures
- Can evolve more freely as they don't cross subsystem boundaries

### External Domain Events

External Domain Events represent significant business occurrences that are relevant across subsystem boundaries. They
form the public contract between subsystems and require careful versioning and stability guarantees.

External Domain Events are:

- Published across subsystem boundaries
- Subject to strict versioning and backward compatibility requirements
- Define the public contract between subsystems
- Require coordination when changes are needed to prevent breaking consumers

## Change Events

Change Events represent technical changes to data or system state. They are lower-level notifications about
modifications to entities or resources within the system (e.g., "CustomerUpdated", "InventoryModified").

Change Events are:

- Technical notifications about state modifications
- Often generated automatically by data stores or persistence layers
- Used to trigger reactive updates, maintain read models, or synchronize caches
- Distinguished from Domain Events by their technical rather than business-focused nature

The distinction between Domain Events and Change Events helps maintain clean separation between business logic and
technical implementation concerns.
