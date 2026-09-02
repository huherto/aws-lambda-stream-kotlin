package io.github.huherto.awsLambdaStream.java;

import io.github.huherto.awsLambdaStream.EventCodec;
import io.github.huherto.awsLambdaStream.UnitOfWork;
import io.github.huherto.awsLambdaStream.flavors.CdcPipeline;
import io.github.huherto.awsLambdaStream.flavors.CollectPipeline;
import io.github.huherto.awsLambdaStream.flavors.UpdatePipeline;
import io.github.huherto.awsLambdaStream.sinks.EventPublisher;
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore;
import kotlinx.coroutines.flow.Flow;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PipelineBuilderTest {

    @Test
    public void testCdcPipelineBuilder() {
        EventPublisher publisher = new EventPublisher() {
            @Override
            public @NotNull Flow<UnitOfWork> publish(@NotNull Flow<UnitOfWork> uows) {
                return uows;
            }
        };

        CdcPipeline pipeline = CdcPipeline.builder()
                .id("test-cdc")
                .eventPublisher(publisher)
                .toEventJava(uow -> null)
                .build();

        assertEquals("test-cdc", pipeline.getId());
    }

    @Test
    public void testUpdatePipelineBuilder() {
        UpdatePipeline pipeline = UpdatePipeline.builder()
                .id("test-update")
                .eventCodec(new EventCodec() {
                    @Override
                    public io.github.huherto.awsLambdaStream.Event decode(String event) { return null; }
                    @Override
                    public String encode(io.github.huherto.awsLambdaStream.Event event) { return null; }
                })
                .toUpdateRequest(uow -> null)
                .build();

        assertEquals("test-update", pipeline.getId());
    }

    @Test
    public void testCollectPipelineBuilder() {
        EventsMicrostore microstore = new EventsMicrostore() {
            @Override
            public Flow<UnitOfWork> save(Flow<UnitOfWork> uows) { return uows; }
            @Override
            public Flow<UnitOfWork> queryByPk(Flow<UnitOfWork> uows) { return uows; }
        };

        CollectPipeline pipeline = CollectPipeline.builder()
                .id("test-collect")
                .eventsMicrostore(microstore)
                .build();

        assertEquals("test-collect", pipeline.getId());
    }
}
