package org.andrejk.query;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TestUtils {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static List<Map<String, Object>> readJsonFileToListOfMaps(String resourceName) throws IOException {
        return readJsonFile(resourceName, new TypeReference<>() {});
    }

    public static <T> T readJsonFile(String resourceName, TypeReference<T> typeReference) throws IOException {
        return OBJECT_MAPPER.readValue(TestUtils.class.getClassLoader().getResourceAsStream(resourceName), typeReference);
    }
}
