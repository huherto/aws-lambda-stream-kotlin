package org.myorg.urls;

import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class JacksonEventCodecTest {
    @Test
    void testRoundTrip() {
        JacksonEventCodec codec = new JacksonEventCodec();
        Url url = new Url("short", "http://long.com", 0);
        UrlCreatedEvent event = new UrlCreatedEvent(
            "id-1", System.currentTimeMillis(), "pk-1",
            Map.of("tag1", "val1"), null, null, List.of(), url
        );

        String json = codec.encode(event);
        assertNotNull(json);
        assertTrue(json.contains("\"type\":\"UrlCreatedEvent\""));
        assertTrue(json.contains("\"shortUrl\":\"short\""));

        UrlEvent decoded = (UrlEvent) codec.decode(json);
        assertEquals(event.id(), decoded.getId());
        assertEquals(event.entity().shortUrl(), decoded.entity().shortUrl());
        assertTrue(decoded instanceof UrlCreatedEvent);
    }
}
