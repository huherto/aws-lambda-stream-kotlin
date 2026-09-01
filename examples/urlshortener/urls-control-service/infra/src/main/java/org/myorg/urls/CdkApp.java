package org.myorg.urls;

public class CdkApp {
    public static void main(String[] args) {
        software.amazon.awscdk.App app = new software.amazon.awscdk.App();

        ServiceProps serviceProps = new ServiceProps(
            "myorg",
            "urls",
            "urls-control-service",
            Stage.DEV,
            "us-east-1"
        );
        new ControlServiceStack(app, serviceProps);

        app.synth();
    }
}
