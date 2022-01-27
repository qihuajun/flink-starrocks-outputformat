package com.socialtouch.martech.mbasedataprocess.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
public class JsonUtil {

    public static ObjectMapper objectMapper = new ObjectMapper();
    public static Gson gson = new Gson();

    static {
        // 修改LocalDateTime的序列化和反序列化格式
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.registerModule(new JavaTimeModule());
        // 忽略 在json字符串中存在，但是在java对象中不存在对应属性的情况。防止错误
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * 将json转为map
     *
     * @param str
     * @return
     */
    public static Map<String, String> string2Map(String str) {
        ObjectMapper objectMapper = new ObjectMapper();
        return string2Obj(str, new TypeReference<Map<String, String>>() {});
    }

    /**
     * 将json转为clzz类型
     *
     * @param str
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T> T string2Obj(String str, Class<T> clazz) {
        if (StringUtils.isEmpty(str) || clazz == null) {
            return null;
        }
        try {
            return clazz.equals(String.class) ? (T) str : objectMapper.readValue(str, clazz);
        } catch (Exception e) {
            log.warn("Parse String to Object error", e);
            return null;
        }
    }

    /**
     * 将json转为特定对象
     *
     * @param str
     * @param typeReference
     * @param <T>
     * @return
     */
    public static <T> T string2Obj(String str, TypeReference<T> typeReference) {
        if (StringUtils.isEmpty(str) || typeReference == null) {
            return null;
        }
        try {
            return (T)
                    (typeReference.getType().equals(String.class) ? str : objectMapper.readValue(str, typeReference));
        } catch (Exception e) {
            log.error("Parse String to Object error", e);
            return null;
        }
    }

    /**
     * json对象格式的字符串转Map
     *
     * @param jsonObj json格式字符串对象
     * @param clazz Map的value类型
     * @param <T> value类型
     * @return Map 转换的Map
     */
    public static <T> Map<String, T> json2Map(String jsonObj, Class<T> clazz) {
        if (StringUtils.isEmpty(jsonObj) || clazz == null) {
            return Collections.emptyMap();
        }
        try {
            return objectMapper.readValue(jsonObj, new TypeReference<Map<String, T>>() {});
        } catch (Exception e) {
            log.warn("Parse String to Object error", e);
            return Collections.emptyMap();
        }
    }

    /**
     * 将对象转为json字符串
     *
     * @param obj
     * @param <T>
     * @return
     */
    public static <T> String obj2String(T obj) {
        if (obj == null) {
            return null;
        }
        try {
            return obj instanceof String ? (String) obj : objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            log.warn("Parse Object to String error", e);
            return null;
        }
    }

    /**
     * 解析json
     *
     * @param jsonStr
     * @return
     */
    public static JsonNode readTree(String jsonStr) {

        JsonNode jsonNode = null;
        try {
            jsonNode = objectMapper.readTree(jsonStr);
        } catch (IOException e) {
            log.error("Parse String to Tree error", e);
        }
        return jsonNode;
    }

    /** json 转 List */
    public static <T> List<T> jsonToList(String jsonString, Class<T> clazz) throws IOException {
        List<T> list = objectMapper.readValue(jsonString, new TypeReference<List<T>>() {});
        return list;
    }

    /** json 转 List */
    public static <T> List<T> jsonToList(Class<T> clazz, String jsonString) {
        try {
            return objectMapper.readValue(jsonString, getCollectionType(List.class, clazz));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取泛型的Collection Type
     *
     * @param collectionClass 泛型的Collection
     * @param elementClasses 实体bean
     * @return JavaType Java类型
     */
    private static JavaType getCollectionType(Class<?> collectionClass, Class<?>... elementClasses) {
        return objectMapper.getTypeFactory().constructParametricType(collectionClass, elementClasses);
    }

    /**
     * 将字符串json数组转换为list
     *
     * @param jsonArray json格式的字符串数组
     * @param <T> 类型
     * @param clazz class对象,指定泛型类型
     * @return List
     * @throws IOException
     */
    public static <T> List<List<T>> jsonArrayToList(String jsonArray, Class<T> clazz) throws IOException {
        return objectMapper.readValue(jsonArray, new TypeReference<List<List<T>>>() {});
    }

    /**
     * 将字符串json数组转换为list
     *
     * @param jsonArray json格式的字符串数组
     * @param <T> 类型
     * @param clazz class对象,指定泛型类型
     * @return List
     * @throws IOException
     */
    public static <T> List<Map<String, T>> jsonStrToList(String jsonArray, Class<T> clazz) throws IOException {
        return objectMapper.readValue(jsonArray, new TypeReference<List<Map<String, T>>>() {});
    }

    /**
     * 对象转Map
     *
     * @param obj 对象
     * @param clazz Map value类型
     * @param <V> Map value类型
     * @return Map
     */
    public static <V> Map<String, V> obj2Map(Object obj, Class<V> clazz) {
        return objectMapper.convertValue(obj, new TypeReference<Map<String, V>>() {});
    }

    /**
     * 将json转化为list
     *
     * @return
     */
    public static List<String> string2List(String str) {
        return string2Obj(str, new TypeReference<List<String>>() {
        });
    }
}
