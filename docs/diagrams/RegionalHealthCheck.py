from diagrams import Diagram, Cluster, Edge
from diagrams.aws.compute import Lambda
from diagrams.aws.database import Dynamodb
from diagrams.aws.storage import S3
from diagrams.aws.integration import SNS, SQS, Eventbridge
from diagrams.aws.analytics import KinesisDataStreams
from diagrams.aws.network import Route53, APIGateway
from diagrams.aws.management import Cloudwatch, CloudwatchAlarm

graph_attr = {
    "fontsize": "20",
    "bgcolor": "transparent"
}

with Diagram("Regional Health Check Architecture", 
             filename="RegionalHealthCheck",
             show=False, 
             direction="LR", 
             graph_attr=graph_attr):
    
    with Cluster("Tracer Flow Cycle"):
        api = APIGateway("Check Health API")
        ddb = Dynamodb("Tracer Table")
        ddb_trigger = Lambda("DynamoDB Trigger")
        s3 = S3("Tracer Bucket")
        sns = SNS("SNS Topic")
        sqs = SQS("SQS Queue")
        s3_trigger = Lambda("S3 Trigger")
        eb = Eventbridge("Event Bus")
        kinesis = KinesisDataStreams("Kinesis Stream")
        kinesis_trigger = Lambda("Kinesis Trigger")

        api >> Edge(label="Update") >> ddb
        ddb >> Edge(label="Stream") >> ddb_trigger
        ddb_trigger >> Edge(label="Put Object") >> s3
        s3 >> Edge(label="Notify") >> sns
        sns >> Edge(label="Deliver") >> sqs
        sqs >> Edge(label="Trigger") >> s3_trigger
        s3_trigger >> Edge(label="Put Event") >> eb
        eb >> Edge(label="Put Record") >> kinesis
        kinesis >> Edge(label="Trigger") >> kinesis_trigger
        kinesis_trigger >> Edge(label="Update") >> ddb

    with Cluster("Observability"):
        canary = Cloudwatch("Synthetics Canary")
        apig_alarm = CloudwatchAlarm("APIG 5xx Alarm")
        ddb_alarm = CloudwatchAlarm("DDB System Error Alarm")
        
        r53_apig = Route53("R53 APIG Health Check")
        r53_ddb = Route53("R53 DDB Health Check")
        r53_reg = Route53("Regional Health Check")
        
        composite = CloudwatchAlarm("Regional Composite Alarm")

        canary >> Edge(label="HTTP Probe", style="dotted") >> api
        api >> Edge(label="Metrics", style="dotted") >> apig_alarm
        ddb >> Edge(label="Metrics", style="dotted") >> ddb_alarm
        
        apig_alarm >> r53_apig
        ddb_alarm >> r53_ddb
        
        r53_apig >> r53_reg
        r53_ddb >> r53_reg
        
        apig_alarm >> composite
        ddb_alarm >> composite

    with Cluster("Event Auditing"):
        events_logs = Cloudwatch("Log Group: Events")
        faults_logs = Cloudwatch("Log Group: Faults")
        eb >> Edge(label="Rule: Default") >> events_logs
        eb >> Edge(label="Rule: Faults") >> faults_logs
