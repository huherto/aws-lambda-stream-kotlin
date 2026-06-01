
# Architectural Approach

Follow a structured approach to application architecture.

## Architectural Structure

A system is composed of one or more subsystems. Each subsystem is composed of one or more services.

Services communicate with each other through events. The event format defines the contract between services.

Similarly, subsystems communicate with each other through events or APIs. These communication mechanisms must have stable, well-defined contracts.

## Events
Events are the primary mechanism for communication between services and subsystems. They are used to trigger actions, coordinate business processes, and facilitate communication between services and subsystems.

### Domain Events

Domain Events represent significant business occurrences within the system. They capture facts about what has happened
in the domain and are named in the past tense to reflect completed actions (e.g., "OrderPlaced", "PaymentProcessed", "
CustomerRegistered").

Domain Events are:

- Immutable records of business facts
- Published when significant state changes occur
- Used to communicate business-level changes between services and subsystems
- The foundation for event-driven architecture and event sourcing patterns

#### Internal Domain Events

Internal Domain Events represent significant business occurrences that are relevant within a single subsystem. They are
used for communication between services within the same subsystem boundary and are not exposed to external subsystems.

Internal Domain Events are:

- Scoped to a single subsystem
- Used to coordinate business processes within subsystem boundaries
- May have subsystem-specific payload structures
- Can evolve more freely as they don't cross subsystem boundaries

#### External Domain Events

External Domain Events represent significant business occurrences that are relevant across subsystem boundaries. They
form the public contract between subsystems and require careful versioning and stability guarantees.

External Domain Events are:

- Published across subsystem boundaries
- Subject to strict versioning and backward compatibility requirements
- Define the public contract between subsystems
- Require coordination when changes are needed to prevent breaking consumers

### Change Events

Change Events represent technical changes to data or system state. They are lower-level notifications about
modifications to entities or resources within the system (e.g., "CustomerUpdated", "InventoryModified").

Change Events are:

- Technical notifications about state modifications
- Often generated automatically by data stores or persistence layers
- Used to trigger reactive updates, maintain read models, or synchronize caches
- Distinguished from Domain Events by their technical rather than business-focused nature

The distinction between Domain Events and Change Events helps maintain clean separation between business logic and
technical implementation concerns.



## Service Types

### Control Services

Control Services orchestrate the flow of events within a subsystem.

They coordinate business processes, determine how events should be handled, and trigger the appropriate downstream actions.

A Control Service includes three main components:

- A Listener Lambda function that receives and processes incoming events.
- A DynamoDB table that stores events for correlation and evaluation.
- A Trigger Lambda function that correlates stored events, evaluates, and emits higher-order events to initiate downstream actions.

![Control Service Diagram](images/ControlService.svg)


### Event Hubs

Event Hubs receive events from components within a subsystem and publish them to Kinesis Streams so they can be consumed by other components.

An Event Hub is typically composed of:

- AWS EventBridge
- One or more Kinesis Streams

Event Hubs provide a central point for event routing and distribution within a subsystem.

### External Service Gateways

External Service Gateways provide integration points between the sub-system and external services.

External Service Gateways act as an Anticorruption Layer (ACL), translating between internal events, external events, and external APIs. This prevents external models or contracts from leaking into the internal architecture.

They may receive events from other services, publish events to other other services, or interact with external APIs.

External Service Gateways act as an Anticorruption Layer (ACL), translating between internal events, external events, and external APIs. This prevents external models or contracts from leaking into the internal architecture.

There are two types of External Service Gateways:
- *Ingress Gateways* – These services are used to receive events from external systems and publish them to the internal system. They convert the external event format to the internal event format.
- *Egress Gateways* – These services are used to publish events to external systems. They convert the internal event format to the external event format. Or they convert an internal event to a call to an external API.

### Backends for Frontends (BFFs)

BFFs provide APIs to the outside world.  They expose client-specific interfaces and adapt internal system capabilities to the needs of external consumers such as web applications, mobile applications, or third-party clients.

### Fault Monitoring Services


