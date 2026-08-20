package io.pendulum.core.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Payload serialization.
 *
 * <p>The important thing about this class is what it refuses to be. Java native serialization is
 * never used for job payloads — deserialization of untrusted data is one of the most exploited
 * vulnerability classes in the JVM ecosystem, and a jobs table is precisely an untrusted input
 * channel: anything that can enqueue can choose the bytes a worker will deserialize. JSON bound to
 * a type the handler names explicitly gives an attacker no gadget chain to reach.
 *
 * <p>For the same reason polymorphic type handling ({@code @JsonTypeInfo} with class names,
 * {@code enableDefaultTyping}) stays off: it re-creates the class-name-in-the-payload problem in
 * JSON clothing.
 */
public final class JsonPayloads {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            // Unknown fields are tolerated so a producer deploying a new field does not break every
            // consumer that has not been redeployed yet. Version skew is the normal state of a
            // fleet mid-deploy, not an exceptional one.
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private JsonPayloads() {
    }

    public static String toJson(Object payload) {
        try {
            return MAPPER.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("payload is not serializable to JSON", e);
        }
    }

    /**
     * Bind a payload to a type the caller names. There is no "read whatever type the payload says
     * it is" variant, and that omission is the point.
     */
    public static <T> T fromJson(String json, Class<T> type) {
        try {
            return MAPPER.readValue(json, type);
        } catch (JsonProcessingException e) {
            // Malformed payloads never succeed on retry, so this surfaces as a terminal error.
            throw new IllegalArgumentException(
                    "payload could not be read as " + type.getSimpleName(), e);
        }
    }
}
