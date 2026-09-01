package org.myorg.urls;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import io.github.huherto.awsLambdaStream.UnitOfWork;
import kotlin.coroutines.EmptyCoroutineContext;
import kotlinx.coroutines.BuildersKt;
import kotlinx.coroutines.flow.FlowKt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class UrlBffHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
    private static final Logger logger = LoggerFactory.getLogger(UrlBffHandler.class);
    private final UrlBffContainer container;

    public UrlBffHandler() {
        this.container = UrlBffContainer.build();
    }

    public UrlBffHandler(UrlBffContainer container) {
        this.container = container;
    }

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent input, Context context) {
        logger.info("Received request: {} {}", input.getHttpMethod(), input.getResource());

        try {
            if ("POST".equals(input.getHttpMethod()) && "/urls".equals(input.getResource())) {
                return createUrl(input);
            }
            if ("DELETE".equals(input.getHttpMethod()) && "/urls/{shortUrl}".equals(input.getResource())) {
                return deleteUrl(input);
            }
            if ("PATCH".equals(input.getHttpMethod()) && "/urls/{shortUrl}".equals(input.getResource())) {
                return updateUrl(input);
            }
            if ("GET".equals(input.getHttpMethod()) && "/{shortUrl}".equals(input.getResource())) {
                return redirectUrl(input);
            }
        } catch (Exception e) {
            logger.error("Error handling request", e);
            return errorResponse(500, e.getMessage());
        }

        return errorResponse(404, "Not Found");
    }

    private APIGatewayProxyResponseEvent createUrl(APIGatewayProxyRequestEvent input) {
        Url request = JacksonEventCodec.INSTANCE.decode(input.getBody(), Url.class);
        String shortUrl = request.shortUrl();
        if (shortUrl == null || shortUrl.isEmpty()) {
            shortUrl = UUID.randomUUID().toString().substring(0, 8);
        }
        Url url = new Url(shortUrl, request.longUrl(), 0);
        container.urlDao.saveUrl(url);
        return jsonResponse(201, url);
    }

    private APIGatewayProxyResponseEvent deleteUrl(APIGatewayProxyRequestEvent input) {
        String shortUrl = input.getPathParameters().get("shortUrl");
        container.urlDao.deleteUrl(shortUrl);
        return jsonResponse(204, null);
    }

    private APIGatewayProxyResponseEvent updateUrl(APIGatewayProxyRequestEvent input) {
        String shortUrl = input.getPathParameters().get("shortUrl");
        Url request = JacksonEventCodec.INSTANCE.decode(input.getBody(), Url.class);
        Url existing = container.urlDao.getUrl(shortUrl);
        if (existing == null) {
            return errorResponse(404, "URL not found");
        }
        Url updated = new Url(shortUrl, request.longUrl(), existing.accessCount());
        container.urlDao.saveUrl(updated);
        return jsonResponse(200, updated);
    }

    private APIGatewayProxyResponseEvent redirectUrl(APIGatewayProxyRequestEvent input) {
        String shortUrl = input.getPathParameters().get("shortUrl");
        Url url = container.urlDao.getUrl(shortUrl);
        if (url == null) {
            return errorResponse(404, "URL not found");
        }

        // Emit Accessed Event
        UrlAccessedEvent event = new UrlAccessedEvent(
            UUID.randomUUID().toString(),
            System.currentTimeMillis(),
            shortUrl,
            Map.of(),
            null,
            null,
            List.of(),
            url
        );
        
        try {
            BuildersKt.runBlocking(EmptyCoroutineContext.INSTANCE, (scope, continuation) ->
                FlowKt.collect(container.eventPublisher.publish(FlowKt.flowOf(new UnitOfWork(null, null, event))), continuation)
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        return new APIGatewayProxyResponseEvent()
                .withStatusCode(302)
                .withHeaders(Map.of("Location", url.longUrl()));
    }

    private APIGatewayProxyResponseEvent jsonResponse(int statusCode, Object body) {
        String bodyString = body != null ? JacksonEventCodec.INSTANCE.encodeObject(body) : "";
        return new APIGatewayProxyResponseEvent()
                .withStatusCode(statusCode)
                .withHeaders(Map.of("Content-Type", "application/json"))
                .withBody(bodyString);
    }

    private APIGatewayProxyResponseEvent errorResponse(int statusCode, String message) {
        return jsonResponse(statusCode, Map.of("message", message));
    }
}
