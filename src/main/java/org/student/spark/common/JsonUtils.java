package org.student.spark.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.util.HashMap;
import java.util.Map;

public class JsonUtils {

    private static final JsonUtils DEFAULT_SERIALIZER;

    static {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        DEFAULT_SERIALIZER = new JsonUtils(mapper);
    }

    public static JsonUtils serializer() {
        return DEFAULT_SERIALIZER;
    }

    private JsonUtils(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    private final ObjectMapper mapper;

    public <T> T fromJson(String json, TypeReference<T> typeRef) {
        try {
            return mapper.readValue(json, typeRef);
        } catch (Exception e) {
            throw new DataFlowException(e);
        }
    }

    public Map<String,Object> jsonToMap(String json) {
        try {
            return mapper.readValue(json, HashMap.class);
        } catch (JsonProcessingException e) {
            throw new DataFlowException(e);
        }
    }

    public String writeValueAsString(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        } catch (Exception e) {
            throw new DataFlowException(e);
        }
    }
}
