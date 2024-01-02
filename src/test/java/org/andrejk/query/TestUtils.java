package org.andrejk.query;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class TestUtils {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static <T> T readJsonFile(String resourceName, TypeReference<T> typeReference) throws IOException {
        return OBJECT_MAPPER.readValue(TestUtils.class.getClassLoader().getResourceAsStream(resourceName), typeReference);
    }
}
