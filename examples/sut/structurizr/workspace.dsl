workspace {

    !const STEEL_BLUE #1168bd
    !const WHITE #ffffff
    !const NAVY_BLUE #08427b
    !const SKY_BLUE #438dd5
    !const LIGHT_SKY_BLUE #85bbf0
    !const BLACK #000000

    !const AWS_LIGHT_BG      "#F2F4F4"
    !const AWS_DARK_FG       "#232F3E"
    !const AWS_MUTED_FG      "#546E7A"

    !const AWS_ORANGE_COMPUTE "#FF9900"
    !const AWS_BLUE_NETWORK   "#2E7D32"
    !const AWS_GREEN_STORAGE  "#3F8624"
    !const AWS_TEAL_DATABASE  "#0077C2"
    !const AWS_BLUE_DATABASE  "#527FFF"
    !const AWS_RED_SECURITY   "#D32F2F"
    !const AWS_PURPLE_APP     "#7B1FA2"
    !const AWS_YELLOW_MGMT    "#FBC02D"
    !const AWS_GREY_INFRA     "#78909C"
    !const AWS_PINK_APP       "#E7157B"

    model {

            archetypes {
               lambda = component {
                    technology "AWS Lambda"
                    tags "Amazon Web Services - Lambda"
                }

                dynamodb = component {
                    technology "AWS DynamoDb"
                    tags "Amazon Web Services - DynamoDB Table"
                }

                service = container
            }
        user = person "User" "A user of the SUT system."

        robot = person "Canary" "" {
            tags "Amazon Web Services - CloudWatch Synthetics"
        }

        sut = softwareSystem "SUT Example" "AWS Lambda stream processing example system." {

            eventHub = service "Event Hub" "Central event distribution and streaming." "AWS EventBridge, Kinesis" {
                eventBus = component "Event Bus" "Central event distribution bus." "AWS EventBridge" {
                    tags "Amazon Web Services - EventBridge"
                }
                eventStream = component "Event Stream" "High-volume event stream for downstream consumers." "AWS Kinesis" {
                    tags "Amazon Web Services - Kinesis"
                }
            }

            controlService = service "Control Service" "Manages control-related events and state." "Kotlin, AWS Lambda, DynamoDB" {
                controlTable = component "Events Table" "Stores control event state." "AWS DynamoDB" {
                    tags "Amazon Web Services - DynamoDB Table"
                }
                controlTrigger = component "Control Trigger" "Processes table changes and publishes to the central EventBus." "AWS Lambda" {
                    tags "Amazon Web Services - Lambda"
                }
                controlListener = component "Control Listener" "Consumes events from the central Kinesis stream and updates the state." "AWS Lambda" {
                    tags "Amazon Web Services - Lambda"
                }
            }

            shipmentBff = service "Shipment BFF" "Backend for Frontend for shipment management." "Kotlin, AWS Lambda, DynamoDB" {
                shipmentApi = lambda "Shipment API" "REST API for managing shipments." "AWS Lambda" {
                    tags "Amazon Web Services - Lambda"
                }
                shipmentTable = dynamodb "Shipments Table" "Stores shipment data." "AWS DynamoDB" {
                    tags "Amazon Web Services - DynamoDB Table"
                }
                shipmentTrigger = component "Shipment Trigger" "Processes table changes and publishes to the central EventBus." "AWS Lambda" {
                    tags "Amazon Web Services - Lambda"
                }
                shipmentListener = component "Shipment Listener" "Consumes events from the central Kinesis stream and updates shipment data." "AWS Lambda" {
                    tags "Amazon Web Services - Lambda"
                }
            }

            eventLake = service "Event Lake" "Archives all system events for long-term storage." "AWS Firehose, S3" {
                lakeFirehose = component "Event Lake Firehose" "Ingests events from EventBridge and delivers to S3." "AWS Kinesis Firehose" {
                    tags "Amazon Web Services - Kinesis Firehose"
                }
                lakeBucket = component "Event Lake Bucket" "Stores archived events." "AWS S3" {
                    tags "Amazon Web Services - Simple Storage Service"
                }
            }

            faultMonitor = service "Event Fault Monitor" "Monitors event processing faults and notifies via SNS." "Kotlin, AWS Firehose, Lambda, SNS" {
                faultFirehose = component "Fault Firehose" "Ingests fault events from EventBridge." "AWS Kinesis Firehose" {
                    tags "Amazon Web Services - Kinesis Firehose"
                }
                faultTransform = component "Fault Transform" "Transforms and filters fault events." "AWS Lambda" {
                    tags "Amazon Web Services - Lambda"
                }
                faultBucket = component "Fault Bucket" "Stores fault event logs." "AWS S3" {
                    tags "Amazon Web Services - Simple Storage Service"
                }
                faultTopic = component "Fault Topic" "SNS Topic for fault notifications." "AWS SNS" {
                    tags "Amazon Web Services - Simple Notification Service"
                }
                faultQueue = component "Fault Queue" "SQS Queue for fault verification." "AWS SQS" {
                    tags "Amazon Web Services - Simple Queue Service"
                }
            }

            healthCheck = container "Regional Health Check" "Self-contained regional health monitoring loop." "Kotlin, AWS Lambda, DynamoDB, S3, SNS, SQS" {
                healthApi = component "Health API" "Endpoint to trigger regional health checks." "AWS Lambda" {
                    tags "Amazon Web Services - Lambda"
                }
                healthTable = component "Health Table" "Stores health check state and results." "AWS DynamoDB" {
                    tags "Amazon Web Services - DynamoDB Table"
                }
                healthDbTrigger = component "Health DB Trigger" "Processes health table changes." "AWS Lambda" {
                    tags "Amazon Web Services - Lambda"
                }
                healthBucket = component "Health Bucket" "Stores health check artifacts." "AWS S3" {
                    tags "Amazon Web Services - Simple Storage Service"
                }
                healthTopic = component "Health Topic" "SNS Topic for health check flow notifications." "AWS SNS" {
                    tags "Amazon Web Services - Simple Notification Service"
                }
                healthQueue = component "Health Queue" "SQS Queue for health check flow verification." "AWS SQS" {
                    tags "Amazon Web Services - Simple Queue Service"
                }
                healthS3Trigger = component "Health S3 Trigger" "Processes S3 events and publishes to local EventBus." "AWS Lambda" {
                    tags "Amazon Web Services - Lambda"
                }
                healthBus = component "Health EventBus" "Local EventBridge bus for health check flow." "AWS EventBridge" {
                    tags "Amazon Web Services - EventBridge"
                }
                healthStream = component "Health Kinesis Stream" "Local Kinesis stream for health check flow." "AWS Kinesis" {
                    tags "Amazon Web Services - Kinesis"
                }
                healthKinesisTrigger = component "Health Kinesis Trigger" "Consumes from local stream and updates health table." "AWS Lambda" {
                    tags "Amazon Web Services - Lambda"
                }
            }
        }
        
        # External Relationships
        user -> shipmentApi "Manages shipments via"
        robot -> healthApi "Triggers health checks via"
        
        # Internal Relationships
        eventBus -> eventStream "Forwards events to"
        
        # Control Service Data Flow
        controlTable -> controlTrigger "Triggers on changes"
        controlTrigger -> eventBus "Publishes events to"
        eventStream -> controlListener "Triggers with events"
        controlListener -> controlTable "Updates state in"
        
        # Shipment BFF Data Flow
        shipmentApi -> shipmentTable "Reads/Writes shipment data"
        shipmentTable -> shipmentTrigger "Triggers on changes"
        shipmentTrigger -> eventBus "Publishes events to"
        eventStream -> shipmentListener "Triggers with events"
        shipmentListener -> shipmentTable "Updates shipment data in"
        
        # Event Lake Data Flow
        eventBus -> lakeFirehose "Delivers events to"
        lakeFirehose -> lakeBucket "Stores events in"
        
        # Fault Monitor Data Flow
        eventBus -> faultFirehose "Delivers fault events to"
        faultFirehose -> faultTransform "Uses for transformation"
        faultFirehose -> faultBucket "Stores fault logs in"
        faultTransform -> faultTopic "Publishes faults to"
        faultTopic -> faultQueue "Forwards notifications to"
        
        # Regional Health Check Data Flow (Tracer Loop)
        healthApi -> healthTable "Initiates health check in"
        healthTable -> healthDbTrigger "Triggers on record creation"
        healthDbTrigger -> healthBucket "Writes artifact to"
        healthBucket -> healthTopic "Notifies on new artifact"
        healthTopic -> healthQueue "Forwards notification to"
        healthQueue -> healthS3Trigger "Triggers processing of"
        healthS3Trigger -> healthBus "Publishes health event to"
        healthBus -> healthStream "Forwards health event to"
        healthStream -> healthKinesisTrigger "Triggers on health event"
        healthKinesisTrigger -> healthTable "Completes health check in"
    }

    views {
        systemContext sut "SystemContext" {
            include *
        }
        
        container sut "Containers" {
            include *
        }
        
        component eventHub "EventHubComponents" {
            include *
        }
        
        component controlService "ControlServiceComponents" {
            include *
        }
        
        component shipmentBff "ShipmentBffComponents" {
            include *
        }
        
        component healthCheck "HealthCheckComponents" {
            include *
        }

        component faultMonitor "FaultMonitorComponents" {
             include *
        }

        component eventLake "EventLakeComponents" {
              include *
        }


        theme https://static.structurizr.com/themes/amazon-web-services-2023.01.31/theme.json

        styles {
            element "Element" {
                background ${STEEL_BLUE}
                color ${WHITE}
            }
            element "Person" {
                shape Person
                background ${NAVY_BLUE}
            }
            element "Robot" {
                shape Robot
                background ${NAVY_BLUE}
            }
            element "Container" {
                background ${SKY_BLUE}
            }
            element "Component" {
                background ${LIGHT_SKY_BLUE}
                color ${BLACK}
            }
            element "Amazon Web Services - Lambda" {
                background ${WHITE}
                color ${AWS_ORANGE_COMPUTE}
            }
            element "Amazon Web Services - EventBridge" {
                background ${WHITE}
                color ${AWS_PINK_APP}
            }
            element "Amazon Web Services - Kinesis" {
                background ${WHITE}
                color ${AWS_PURPLE_APP}
            }
            element "Amazon Web Services - DynamoDB Table" {
                background ${WHITE}
                color ${AWS_BLUE_DATABASE}
                #color ${AWS_PURPLE_APP}
                shape cylinder
            }
            element "Amazon Web Services - Kinesis Firehose" {
                background ${WHITE}
                color ${AWS_PURPLE_APP}
            }
            element "Amazon Web Services - Simple Storage Service" {
                background ${WHITE}
                color ${AWS_GREEN_STORAGE}
            }
            element "Amazon Web Services - Simple Notification Service" {
                background ${WHITE}
                color ${AWS_PINK_APP}
            }
            element "Amazon Web Services - Simple Queue Service" {
                background ${WHITE}
                color ${AWS_PINK_APP}
            }
            element "Amazon Web Services - CloudWatch Synthetics" {
                shape Robot
                background ${WHITE}
                color ${AWS_PURPLE_APP}
            }
        }
    }
}
