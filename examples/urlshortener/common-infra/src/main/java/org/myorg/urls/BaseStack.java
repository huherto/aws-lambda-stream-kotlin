package org.myorg.urls;

import software.amazon.awscdk.Stack;
import software.constructs.Construct;

public class BaseStack extends Stack {
    private final ServiceProps serviceProps;

    public BaseStack(Construct scope, ServiceProps serviceProps) {
        super(scope, serviceProps.service() + "-" + serviceProps.stage());
        this.serviceProps = serviceProps;
    }

    public String org() { return serviceProps.org(); }
    public String subsys() { return serviceProps.subsys(); }
    public String service() { return serviceProps.service(); }
    public Stage stage() { return serviceProps.stage(); }
    public String regionName() { return serviceProps.region(); }
}
