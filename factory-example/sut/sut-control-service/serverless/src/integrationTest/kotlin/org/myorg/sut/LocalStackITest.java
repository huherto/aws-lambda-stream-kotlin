package org.myorg.sut;

import aws.sdk.kotlin.services.kinesis.KinesisClient;
import org.junit.Test;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

public class LocalStackITest {

    static LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:stable"))
            .withServices(
                    LocalStackContainer.Service.KINESIS,
                    LocalStackContainer.Service.DYNAMODB,
                    LocalStackContainer.Service.LAMBDA);

    @BeforeAll
    static void setUp() {
        localstack.start();
    }

}
