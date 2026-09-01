package org.myorg.urls;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import io.github.huherto.awsLambdaStream.java.Handlers;
import io.github.huherto.awsLambdaStream.java.PipelineRunner;

public class ControlTriggerHandler implements RequestHandler<DynamodbEvent, Void> {
    private final ControlTriggerContainer container;

    public ControlTriggerHandler() {
        this.container = ControlTriggerContainer.build();
    }

    public ControlTriggerHandler(ControlTriggerContainer container) {
        this.container = container;
    }

    @Override
    public Void handleRequest(DynamodbEvent input, Context context) {
        new PipelineRunner<DynamodbEvent>(container.assembler)
                .headFlow(container.dynamoDbAdapter::fromDynamoDB)
                .transformer(Handlers::collectMetrics)
                .run(input);
        return null;
    }
}
