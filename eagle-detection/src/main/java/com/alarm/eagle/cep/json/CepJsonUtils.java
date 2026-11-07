package com.alarm.eagle.cep.json;

import com.alarm.eagle.cep.json.spec.GraphSpec;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JSON ↔ Pattern bidirectional conversion utility.
 *
 * <p>Core pipeline:
 * <pre>
 *   Pattern —(GraphSpec.fromPattern)→ GraphSpec —(ObjectMapper)→ JSON String
 *   JSON String —(ObjectMapper)→ GraphSpec —(toPattern)→ Pattern
 * </pre>
 *
 * <p>业务含义：规则以 JSON 字符串形式存储/传输，运行时由本工具还原成 Flink
 * {@link Pattern}，是整个 Dynamic CEP"规则可配置、可热更新"的序列化基础。
 */
public class CepJsonUtils {

    private static final Logger LOG =
            LoggerFactory.getLogger(CepJsonUtils.class);

    /** ★ Configure Jackson ObjectMapper (register polymorphic types + Time deserializer) */
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new SimpleModule()
                    .addDeserializer(Time.class, new TimeDeserializer()))
            .enable(SerializationFeature.INDENT_OUTPUT);

    /** Expose for external use (e.g. JDBCPeriodicPatternProcessorDiscoverer) */
    public static ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    /**
     * Pattern → JSON String (for debugging/development).
     */
    public static String convertPatternToJSONString(
            Pattern<?, ?> pattern) throws JsonProcessingException {
        GraphSpec graphSpec = GraphSpec.fromPattern(pattern);
        return objectMapper.writeValueAsString(graphSpec);
    }

    /**
     * ★ JSON String → Pattern (core method).
     *
     * @param jsonString  GraphSpec JSON 字符串（一条规则的完整定义）
     * @param classLoader 用户代码 ClassLoader（用于加载 CLASS 类型条件）
     * @return 还原出的 Flink Pattern 对象（链式 Pattern）
     * @throws Exception JSON 解析失败或 Pattern 重建失败时抛出
     */
    @SuppressWarnings("unchecked")
    public static <T> Pattern<T, ?> convertJSONStringToPattern(
            String jsonString, ClassLoader classLoader) throws Exception {
        if (classLoader == null) {
            LOG.warn("classLoader is null, using current thread context classLoader.");
            classLoader = Thread.currentThread().getContextClassLoader();
        }
        GraphSpec graphSpec = objectMapper.readValue(jsonString, GraphSpec.class);
        LOG.debug("Deserialized GraphSpec: {}", graphSpec);
        return (Pattern<T, ?>) graphSpec.toPattern(classLoader);
    }

    /**
     * JSON String → GraphSpec (without rendering to Pattern).
     */
    public static GraphSpec convertJSONStringToGraphSpec(
            String jsonString) throws JsonProcessingException {
        return objectMapper.readValue(jsonString, GraphSpec.class);
    }

    /**
     * GraphSpec → JSON String.
     */
    public static String convertGraphSpecToJSONString(
            GraphSpec graphSpec) throws JsonProcessingException {
        return objectMapper.writeValueAsString(graphSpec);
    }
}
