package org.myorg.urls;

public class CdkApp {
    public static void main(String[] args) {
        software.amazon.awscdk.App app = new software.amazon.awscdk.App();

        ServiceProps serviceProps = new ServiceProps(
            "myorg",
            "urls",
            "urls-event-hub",
            Stage.DEV,
            "us-east-1"
        );
        new EventHubStack(app, serviceProps);

        app.synth();
    }
}
