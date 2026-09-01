package org.myorg.urls;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.huherto.awsLambdaStream.Event;
import io.github.huherto.awsLambdaStream.EventCodec;
import org.jetbrains.annotations.NotNull;

public class JacksonEventCodec implements EventCodec {
    public static final JacksonEventCodec INSTANCE = new JacksonEventCodec();

    private final ObjectMapper objectMapper;

    public JacksonEventCodec() {
        this.objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    @Override
    public Event decode(@NotNull String s) {
        return decode(s, UrlEvent.class);
    }

    public <T> T decode(String s, Class<T> clazz) {
        try {
            return objectMapper.readValue(s, clazz);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to decode: " + s, e);
        }
    }

    @Override
    public String encode(Event event) {
        return encodeObject(event);
    }

    public String encodeObject(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to encode object", e);
        }
    }
}
